package npg_warehouse::loader::elembio::run;

use Moose;
use MooseX::StrictConstructor;
use Readonly;

use npg_tracking::glossary::composition::component::illumina;
use npg_tracking::glossary::composition::factory;
use npg_tracking::glossary::rpt;
use npg_tracking::illumina::run::folder;
use npg_tracking::Schema;
use npg_qc::Schema;
use npg_qc::elembio::run_stats;
use WTSI::DNAP::Warehouse::Schema;
use npg_warehouse::loader::illumina::npg;
use npg_warehouse::loader::illumina::fqc;

with qw/ MooseX::Getopt
         WTSI::DNAP::Utilities::Loggable
         npg_tracking::illumina::run::folder /;

our $VERSION = '0';

Readonly::Scalar my $CONTROL_SAMPLE_NAME_REGEXP => qr/(?:adept)|(?:phix_third)/ismx;
Readonly::Scalar my $HUNDRED => 100;
Readonly::Scalar my $UNASSIGNED_DATA_TAG_INDEX => 0;
Readonly::Scalar my $DIGEST_COLUMN_NAME           => 'id_eseq_product';
Readonly::Scalar my $COMPOSITION_JSON_COLUMN_NAME => 'eseq_composition_tmp';
Readonly::Scalar my $COMPOSITION_OBJECT_KEY       => 'c_object';
Readonly::Scalar my $FLOWCELL_FK_COLUMN_NAME      => 'id_eseq_flowcell_tmp';

=head1 NAME

npg_warehouse::loader::elembio::run

=head1 SYNOPSIS
 
  my $path = 'some/path';
  npg_warehouse::loader::elembio::run->new(
    runfolder_path => $path, id_run => 88
  )->load();

=head1 DESCRIPTION

Uploads (updates or inserts) run, lane and products information to
C<eseq_run_lane_metrics> and  C<eseq_product_metrics> tables  of the
ml warehouse database.

This Moose class, via inheritance from C<npg_tracking::illumina::run::folder>,
has a number of attributes for accessing paths inside the run folder. At the
time of writing only the methods documented below are meaningful and safe to use.

=head1 SUBROUTINES/METHODS

=head2 runfolder_path

Elembio run folder path, including the run folder name. Required.
Inherited from npg_tracking::illumina::run::folder

=head2 run_folder

Run folder name. Inherited from npg_tracking::illumina::run::folder

=head2 id_run

Run ID as recorded in NPG tracking database. Required.

=head2 npg_tracking_schema

DBIx schema object for NPG tracking database schema. Required.
Inherited from npg_tracking::illumina::run::folder

=head2 tracking_run

npg_tracking::Schema::Result::Run object.
Inherited from npg_tracking::illumina::run::folder

=cut

##### Customise inherited attributes

# Amend attributes which we do not want to show up as script's arguments.

my @script_args = qw/runfolder_path id_run/;

my @no_script_arg_attrs =
  grep { ($_ ne $script_args[0]) && ($_ ne $script_args[1]) }
  npg_tracking::illumina::run::folder->meta->get_attribute_list();

has [map {q[+] . $_ } @no_script_arg_attrs] => (metaclass => 'NoGetopt',);

# Annotate script arguments attributes as required.
has [map {q[+] . $_ } @script_args] => (required => 1,);

# Always require tracking db connection.
sub _build_npg_tracking_schema {
  return npg_tracking::Schema->connect;
}

##### End of customisation

=head2 mlwh_schema

DBIx schema object for the warehouse database.

=cut

has 'mlwh_schema' => (
  isa        => 'WTSI::DNAP::Warehouse::Schema',
  metaclass  => 'NoGetopt',
  is         => 'ro',
  lazy_build => 1,
);
sub _build_mlwh_schema {
  return WTSI::DNAP::Warehouse::Schema->connect();
}

=head2 npg_qc_schema

DBIx schema object for the warehouse database.

=cut

has 'npg_qc_schema' => (
  isa        => 'npg_qc::Schema',
  metaclass  => 'NoGetopt',
  is         => 'ro',
  lazy_build => 1,
);
sub _build_npg_qc_schema {
  return npg_qc::Schema->connect();
}

=head2 load

Loads C<eseq_run_lane_metrics> and C<eseq_product_metrics> tables.
Either creates records or updates existing records.

No existing rows are deleted. If anything has changed in the composition of
the sequenced pools, it is advisable to drop all records prior to invoking
this method.

The loader does not load product data for cancelled or stopped early runs.
Runs marked as stopped early will either be analysed and picked up by
the mlwh loader that runs within the analysis pipeline or manually moved to
'run cancelled' status. See C<npg_warehouse::loader::illumina::npg::run_is_cancelled>
for the definition of a cancelled run.

If LIMS batch ID value is set in the tracking database for this run,
an attempt to link eseq_product_metrics table to eseq_flowcell table is
made.

=cut

sub load {
  my $self = shift;

  -d $self->runfolder_path or $self->logger()->logcroak(
    sprintf 'Run folder path %s does not exist', $self->runfolder_path
  );

  my $rl_rs = $self->mlwh_schema->resultset('EseqRunLaneMetric');
  my %column_names = map { $_ => 1 } $rl_rs->result_source->columns;
  my $run_stopped_early =
    $self->tracking_run->current_run_status_description() eq 'run stopped early';
  my $lane_data = $self->_get_run_lane_data(\%column_names, $run_stopped_early);

  my $loader = sub {

    my $have_product_data = 1;

    for my $data (@{$lane_data}) {
      $rl_rs->update_or_create($data);
    }

    if ($lane_data->[0]->{cancelled} || $run_stopped_early ) {
      $have_product_data = 0;
    } else {
      my $pr_rs = $self->mlwh_schema->resultset('EseqProductMetric');
      my $product_data = $self->_get_product_data();
      if (@{$product_data}) {
        for my $data (@{$product_data}) {
          $pr_rs->update_or_create($data);
        }
      } else {
        $have_product_data = 0;
      }
    }

    return $have_product_data;
  };
  my $have_product_data = $self->mlwh_schema->txn_do($loader);
  if (!$have_product_data) {
    $self->logger()->warn('No product data');
  }

  # If we have LIMS reference for a run, we will try to link eseq_product_metrics
  # run data to relevant records in eseq_flowcell table or relink if the foreign
  # keys have been already set.
  my $batch_id = $self->tracking_run->batch_id;
  if ($have_product_data && defined $batch_id) {
    my $link_to_batch = sub {
      return $self->_link2lims($batch_id);
    };
    $self->mlwh_schema->txn_do($link_to_batch);
  } else {
    $self->logger()->warn(
      'No batch ID, cannot link product data for run folder ' .
      $self->runfolder_path . ' run ID ' . $self->id_run
    );
  }

  return;
}

has '_lane_qc_stats' => (
  isa        => 'HashRef',
  is         => 'ro',
  lazy_build => 1,
);
sub _build__lane_qc_stats {
  my $self = shift;

  my $stats = {};
  my $elembio_analysis_path = join q[/], $self->runfolder_path,
    $self->tracking_run->folder_name;
  if (-d $elembio_analysis_path) {
    my $run_stats_file = "$elembio_analysis_path/RunStats.json";
    my $run_manifest_file = "$elembio_analysis_path/RunManifest.json";
    if (-e $run_stats_file && -e $run_manifest_file) {
      my $run_stats = npg_qc::elembio::run_stats::run_stats_from_file(
        $run_manifest_file, $run_stats_file, $self->tracking_run->run_lanes->count()
      );
      $stats = $run_stats->lanes();
    } else {
      $self->logger()->error(
        "Either $run_stats_file or $run_manifest_file does not exist"
      );
    }
  } else {
    $self->logger()->error(
      "Elembio deplexing directory $elembio_analysis_path does not exist"
    );
  }

  return $stats;
}

sub _get_run_lane_data {
  my ($self, $column_names, $run_stopped_early) = @_;

  # Elembio runs and their statuses are registered in the run tracking
  # database exactly in the same way as Illumina runs. The same tables
  # are used. Therefore the code for retrieving run and lane data from
  # the tracking database can be reused for Elembio data.

  my $data_source = npg_warehouse::loader::illumina::npg->new(
    id_run => $self->id_run,
    schema_npg => $self->npg_tracking_schema
  );
  my $qc_outcomes_data_source = npg_warehouse::loader::illumina::fqc->new(
    schema_qc => $self->npg_qc_schema, digests => {}
  );

  my $run_is_cancelled = $data_source->run_is_cancelled();
  my $run_is_paired = $data_source->run_is_paired_read;

  my @run_info = (
    %{$data_source->instrument_info()},
    %{$data_source->dates},
  );

  my $run_lane_info = $data_source->dates4lanes();

  my @per_lane_data = ();

  for my $run_lane ( $self->tracking_run->run_lanes()->all() ) {
    my $lane = $run_lane->position;
    my @lane_data = @run_info;
    if ( exists $run_lane_info->{$lane} ) {
      push @lane_data, %{$run_lane_info->{$lane}};
    }
    if (!$run_is_cancelled) {
      my $rp_key = npg_tracking::glossary::rpt->deflate_rpt(
        {id_run => $self->id_run, position => $lane}
      );
      push @lane_data, %{$qc_outcomes_data_source->retrieve_seq_outcome($rp_key)};
    }

    my %data = @lane_data;
    $data{run_folder_name} = $self->tracking_run->folder_name;
    $data{id_run} = $self->id_run;
    $data{lane} = $lane;
    $data{flowcell_barcode} = $self->tracking_run->flowcell_id;
    $data{paired_read} = $run_is_paired;
    $data{cycles} = $self->tracking_run->actual_cycle_count;
    $data{cancelled} = $run_is_cancelled;
    $data{run_priority} = $self->tracking_run->priority;

    # Tag deplexing data is not available for unfinished runs.
    if (!($run_is_cancelled || $run_stopped_early)) {
      if (exists $self->_lane_qc_stats()->{$lane}) {
        my $lane_qc = $self->_lane_qc_stats()->{$lane};
        $data{tags_decode_percent} = sprintf '%.2f',
          (($lane_qc->num_polonies - $lane_qc->unassigned_reads)/
            $lane_qc->num_polonies) * $HUNDRED;
        $data{num_polonies} = $lane_qc->num_polonies();
      }
    }

    for my $column_name ( keys %data ) {
      exists $column_names->{$column_name} or delete $data{$column_name};
    }

    push @per_lane_data, \%data;
  }

  return \@per_lane_data;
}

sub _get_product_data {
  my $self = shift;

  my @product_data = @{$self->_get_product_qc_stats()};
  my $compositions = {};

  foreach my $product (@product_data) {
    my $lane = $product->{lane};
    my $ti = $product->{tag_index};
    if (!exists $compositions->{$lane}->{$ti}) {
      $compositions->{$lane}->{$ti} =
        $self->_composition4single_component($lane, $ti);
    }
    for my $name (($DIGEST_COLUMN_NAME, $COMPOSITION_JSON_COLUMN_NAME)) {
      $product->{$name} = $compositions->{$lane}->{$ti}->{$name};
    }
  }

  my %digests = map {
    $_->{$DIGEST_COLUMN_NAME} =>
      $compositions->{$_->{lane}}->{$_->{tag_index}}->{$COMPOSITION_OBJECT_KEY}
  } @product_data;

  ######
  # Batch-retrieve QC outcomes for all products and cache.
  #
  # Both Elembio and Illumina QC outcomes are saved to QC database
  # to the same tables. Therefore, it is possible to reuse the code,
  # which was originally written for Illumina data.
  my $qc_outcomes_data_source = npg_warehouse::loader::illumina::fqc->new(
    schema_qc => $self->npg_qc_schema, digests => \%digests
  );
  $qc_outcomes_data_source->retrieve();

  # Copy retrieved QC outcomes to product data.
  foreach my $product (@product_data) {
    my $qc_outcomes = $qc_outcomes_data_source
      ->retrieve_outcomes($product->{$DIGEST_COLUMN_NAME});
    delete $qc_outcomes->{'qc_user'}; # This key is unlikely to exist.
    for my $qc_type (keys %{$qc_outcomes}) {
      $product->{$qc_type} = $qc_outcomes->{$qc_type};
    }
  }

  return \@product_data;
}

sub _get_product_qc_stats {
  my $self = shift;

  my @products = ();

  foreach my $lane (sort keys %{$self->_lane_qc_stats()}) {
    my $lane_stats = $self->_lane_qc_stats()->{$lane};

    # Assign data that didn't decode to tag zero - this is NPG's Illumina
    # convention, which we follow for Elembio platform as well.
    # No sample name, no project info, no tag sequences. 
    my $tag_zero = {
      'id_run' => $self->id_run,
      'lane' => $lane,
      'tag_index' => $UNASSIGNED_DATA_TAG_INDEX,
      'is_sequencing_control' => 0,
      'tag_decode_count' => $lane_stats->unassigned_reads,
      'tag_decode_percent' => _tag_decode_percent(
        $lane_stats->unassigned_reads, $lane_stats->num_polonies),
    };
    push @products, $tag_zero;

    foreach my $sample_stats ( sort { $a->tag_index <=> $b->tag_index }
                               values %{$lane_stats->deplexed_samples} ) {

      my $is_control = $sample_stats->sample_name =~ /$CONTROL_SAMPLE_NAME_REGEXP/smx ? 1 : 0;

      foreach my $barcode_string (sort keys %{$sample_stats->barcodes}) {
        my $lib_stats = $sample_stats->barcodes->{$barcode_string};
        my $product = {
          'id_run' => $self->id_run,
          'lane' => $lane,
          'tag_index' => $sample_stats->tag_index,
          'elembio_samplename' => $sample_stats->sample_name,
          'is_sequencing_control' => $is_control,
          'tag_sequence' => $lib_stats->barcodes->[0],
          'tag2_sequence' => $lib_stats->barcodes->[1],
          'tag_decode_count' => $lib_stats->num_polonies,
          'tag_decode_percent' => _tag_decode_percent(
             $lib_stats->num_polonies, $lane_stats->num_polonies),
        };
        #TODO - set elembio_Project
        push @products, $product;
      }
    }
  }

  return \@products;
}

sub _tag_decode_percent {
  my ($lib_num_polonies, $lane_num_polonies) = @_;
  return $lane_num_polonies ? ($lib_num_polonies/$lane_num_polonies) * $HUNDRED : 0;
}

sub _composition4single_component {
  my ($self, $lane, $tag_index) = @_;

  my $component = npg_tracking::glossary::composition::component::illumina->new(
    id_run => $self->id_run,
    position => $lane,
    tag_index => $tag_index
  );
  my $factory = npg_tracking::glossary::composition::factory->new();
  $factory->add_component($component);
  my $composition = $factory->create_composition();

  return {
    $COMPOSITION_OBJECT_KEY => $composition,
    $DIGEST_COLUMN_NAME     => $composition->digest(),
    $COMPOSITION_JSON_COLUMN_NAME => $composition->freeze()
  };
}

sub _link2lims {
  my ($self, $batch_id) = @_;

  # Do we have any LIMS data for this batch?
  my $fc_rs = $self->mlwh_schema->resultset('EseqFlowcell')
     ->search({id_flowcell_lims => $batch_id});
  my $num_fc_rows = $fc_rs->count();
  if ($num_fc_rows == 0) {
    # Normal for walk-up runs.
    $self->logger()->warn("No LIMS data for batch ID $batch_id");
    return;
  }

  my $na = q[NA];
  my $done = q[done];
  my $lims_data = {};
  while (my $fc_row = $fc_rs->next) {
    my $tag2 = $fc_row->tag2_sequence;
    $tag2 ||= $na;
    $lims_data->{$fc_row->lane}->{$fc_row->tag_sequence}->{$tag2} =
      $fc_row->$FLOWCELL_FK_COLUMN_NAME;
  }

  # Pick up rows for lane-level deplexed data to link.
  my $id_run = $self->id_run;
  my $pr_rs = $self->mlwh_schema->resultset('EseqProductMetric')->search({
    id_run => $id_run,
    lane => { q[!=] => undef},
    tag_sequence => { q[!=] => $UNASSIGNED_DATA_TAG_INDEX },
  });

  # Establish mapping between the rows in eseq_flowcell and eseq_product_metrics
  # rows. If the mapping is partial, link whatever maps and warn.
  my $num2link = $pr_rs->count();
  my $original_num2link = $num2link;

  while (my $pr_row = $pr_rs->next) {
    my $tag2 = $pr_row->tag2_sequence;
    $tag2 ||= $na;
    my $fc_id = $lims_data->{$pr_row->lane}->{$pr_row->tag_sequence}->{$tag2};
    if (defined $fc_id) {
      if ( $fc_id eq $done ) {
        $self->logger()->logcroak('Should not have this');
      }
      $num2link--;
      $pr_row->update({$FLOWCELL_FK_COLUMN_NAME => $fc_id});
      $lims_data->{$pr_row->lane}->{$pr_row->tag_sequence}->{$tag2} = $done;
    }
  }
  if ($num2link) {
    if ($num2link == $original_num2link) {
      $self->logger()->warn("No products for run $id_run is linked to LIMS data");
    } else {
      # OK for controls?
      $self->logger()->warn(
        "$num2link products for run $id_run have not been linked to LIMS data"
      );
    }
  }
  # Should we figure out if there are any unlinked LIMS rows?
  # Not for now, might be a costly exercise for a batch loader.

  return ($original_num2link - $num2link);
}


__PACKAGE__->meta->make_immutable;

1;

__END__

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Carp

=item Moose

=item MooseX::StrictConstructor

=item MooseX::Getopt

=item Readonly

=item WTSI::DNAP::Utilities::Loggable

=item npg_tracking::glossary::rpt

=item WTSI::DNAP::Warehouse::Schema

=item npg_tracking::illumina::run::folder

=item npg_tracking::Schema

=item npg_tracking::glossary::composition::component::illumina

=item npg_tracking::glossary::composition::factory

=item npg_qc::Schema

=item npg_qc::elembio::run_stats

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Marina Gourtovaia E<lt>mg8@sanger.ac.ukE<gt>

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2025 Genome Research Ltd.

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.

=cut
