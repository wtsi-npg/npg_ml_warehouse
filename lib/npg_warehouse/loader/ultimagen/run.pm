package npg_warehouse::loader::ultimagen::run;

use Moose;
use MooseX::StrictConstructor;
use Try::Tiny;
use Readonly;
use Carp;

use npg_tracking::glossary::composition;
use npg_tracking::glossary::composition::component::illumina;

use npg_qc::autoqc::qc_store::options qw/$ALLALL/;
use npg_qc::autoqc::qc_store::query;
use npg_qc::autoqc::qc_store;

use npg_warehouse::loader::illumina::fqc;
use npg_warehouse::loader::illumina::npg;

extends 'npg_warehouse::loader::base';

with qw/ npg_tracking::glossary::run
         npg_qc::ultimagen::sample_retriever
         MooseX::Getopt /;

our $VERSION  = '0';

Readonly::Scalar my $LANE => 1; # TODO - get from npg_qc?
Readonly::Scalar my $UNASSIGNED_DATA_TAG_INDEX => 0;

=head1 NAME

npg_warehouse::loader::ultimagen::run

=head1 SYNOPSIS

Either C<runfolder_path> or C<manifest_path> should be defined.

If only runfolder_path attribute is defined, information about target samples will
be taken from [RUN_ID]_LibraryInfo.xml file in the run folder:

 npg::warehouse::loader::ultimagen::run->new(
  id_run => 4444,
  runfolder_path => '/some/dir'
 )->load();

If both runfolder_path and manifest_path attributes are defined or only manifest_path
attribute is defined, information about target samples will be taken from the manifest.

 npg::warehouse::loader::ultimagen::run->new(
  id_run => 4444,
  manifest_path => '4444_manifest.csv'
 )->load();

npg::warehouse::loader::ultimagen::run->new(
  id_run => 4444,
  manifest_path => '4444_manifest.csv',
  runfolder_path => '/some/dir'
 )->load();

=head1 DESCRIPTION

=head1 SUBROUTINES/METHODS

=head2 id_run

Run id, inherited from C<npg_tracking::glossary::run>, required.

=cut

has '+id_run' => (
  documentation => 'NPG tracking run ID',
);

=head2 runfolder_path

Run folder path, optional. Inherited from C<npg_qc::ultimagen::sample_retriever>.

=head2 manifest_path

Manifest path, optional. Inherited from C<npg_qc::ultimagen::sample_retriever>.

=cut

for my $attr_name (qw/ explain lims_fk_repair verbose
                       logger schema_wh schema_npg schema_qc /) {
  has "+${attr_name}" => (metaclass  => 'NoGetopt',);
}

=head2 BUILD

This method is run at the end of instantiating an instance of this class.
It ensures oe of C<runfolder_path> or C<manifest_path> attributes is set.

=cut

sub BUILD {
  my $self = shift;
  ($self->has_runfolder_path or $self->has_manifest_path) or croak
    'Either runfolder_path or manifest_path attribute should be set';
  return;
}

=head2 load

Loads run and product data to useq_run_metrics and useq_product_metrics mlwh tables.

At the time of writing (January 2026) we have autoqc results for one tag_metrics
result (position 1) and qX_yield results for target samples and the Ultima
Genomics control (if present). All counts, yields and tag sequences come from
these autoqc results.

=cut

sub load {
  my $self = shift;

  my ($run_data, $samples) = $self->_get_data4loading();

  try {
    $self->schema_wh->txn_do( sub {
      $self->schema_wh->resultset('UseqRunMetric')->update_or_create($run_data);
      $self->logger->info('useq_run_metrics table is loaded');;
      for my $sample (@{$samples}) {
        $self->schema_wh->resultset('UseqProductMetric')->update_or_create($sample);
      }
      $self->logger->info('useq_product_metrics table is loaded');
    });
  } catch { # Exit on error.
    my $error = $_;
    $self->logger->logcroak("Failed to load data to mlwh: $error");
  };

  try {
    my $id = $self->schema_wh->resultset('UseqRunMetric')
      ->find({id_run => $self->id_run})->ultimagen_library_pool;
    if ($id) {
      my $num_linked = $self->schema_wh->txn_do( sub {
        return $self->_link2lims($id);
      });
      $self->logger->info("$num_linked products linked to LIMS data");
    } else {
      $self->logger->warn('useq_run_metrics.ultimagen_library_pool is unset, ' .
        'cannot link products to LIMS data');
    }
  } catch { # Do not exit on error.
    my $error = $_;
    $self->logger->error("Error linking products to LIMS data: $error");
  };

  $self->logger->info('Finished loading data to mlwh');

  return;
}

sub _get_data4loading {
  my $self = shift;

  my $autoqc_results = $self->_get_autoqc_results();

  my @run_data = ();
  push @run_data, $self->_get_run_tracking_info();
  push @run_data, $self->_get_run_mqc_outcome();
  push @run_data, $autoqc_results->{run};

  my $data = {};
   # Remap the data into a common hash.
  for my $run_data_entity (@run_data) {
    for my $key (keys %{$run_data_entity}) {
      $data->{$key} = $run_data_entity->{$key};
    }
  }

  # Iterate over target samples which are listed either in LibraryInfo.xml or
  # the manifest, whatever input we use.
  for my $ug_sample (@{$self->get_samples()}) {
    my $tag_index = $ug_sample->tag_index();
    if (!exists $autoqc_results->{samples}->{$tag_index}) {
      $self->logger->logwarn("No autoqc results for tag index $tag_index");
    }
    $autoqc_results->{samples}->{$tag_index}->{ultimagen_index_label} =
      $ug_sample->index_label;
    $autoqc_results->{samples}->{$tag_index}->{ultimagen_sample_id} =
      $ug_sample->id;
    $autoqc_results->{samples}->{$tag_index}->{ultimagen_library_name} =
      $ug_sample->library_name;
    $autoqc_results->{samples}->{$tag_index}->{ultimagen_index_sequence} =
      $ug_sample->index_sequence;
  }
  my $mqc_outcomes = exists $autoqc_results->{digests} ?
    $self->_get_product_mqc_outcomes($autoqc_results->{digests}) : {};

  # Fill in any missing sample data.
  my @samples = ();
  while ( my ($tag_index, $sample) = each %{$autoqc_results->{samples}} ) {

    $sample->{tag_index} = $tag_index;
    $sample->{id_run} = $self->id_run;

    my $product_id = $sample->{id_useq_product};
    if (!$product_id) { # This will be the case of tag zero or any other tag
                        # if qX_yield results are not available.
      $product_id = $self->_generate_composition($tag_index)->digest();
      $sample->{id_useq_product} = $product_id;
    }

    my $qc_outcomes = $mqc_outcomes->{$product_id} || {};
    delete $qc_outcomes->{'qc_user'}; # This key is unlikely to exist.
    for my $qc_type (keys %{$qc_outcomes}) {
      $sample->{$qc_type} = $qc_outcomes->{$qc_type};
    }
    push @samples, $sample;
  }

  return ($data, \@samples);
}

sub _get_run_tracking_info {
  my $self = shift;

  my $tracking_run = $self->schema_npg()->resultset('Run')->find($self->id_run);
  $tracking_run or $self->logger->logcroak(sprintf
    'Record for run %i is not in the tracking database', $self->id_run);

  my $data = {};
  $data->{id_run} = $self->id_run;
  $data->{ultimagen_run_id} = $tracking_run->flowcell_id;
  $data->{ultimagen_library_pool} = $tracking_run->batch_id;
  $data->{run_folder_name} = $tracking_run->folder_name;

  my $instrument = $tracking_run->instrument;
  $data->{instrument_name} = $instrument->name;
  $data->{instrument_external_name} = $instrument->external_name;
  $data->{instrument_model} = $instrument->instrument_format->model;

  $data->{run_priority} = $tracking_run->priority;
  my $npg_retriever = npg_warehouse::loader::illumina::npg->new(
    id_run => $self->id_run, schema_npg => $self->schema_npg()
  );
  $data->{cancelled} = $npg_retriever->run_is_cancelled;

  my $rs = $self->schema_npg->resultset('RunStatus')->search(
    { 'me.id_run' => $self->id_run },
    { 'order_by' => [{-asc => q[me.date]}] }
  );
  while (my $run_status = $rs->next()) {
    my $status_desc = $run_status->description();
    if ($status_desc eq 'run in progress') {
      $data->{run_in_progress} = $run_status->date;
    } elsif ($status_desc eq 'run archived') {
      $data->{run_archived} = $run_status->date;
    }
  }

  return $data;
}

sub _get_run_mqc_outcome {
  my $self = shift;

  my $qc_outcomes_data_source = npg_warehouse::loader::illumina::fqc->new(
    schema_qc => $self->schema_qc, digests => {}
  );
  $qc_outcomes_data_source->retrieve();
  my $rp_key = npg_tracking::glossary::rpt->deflate_rpt(
    {id_run => $self->id_run, position => $LANE}
  );

  return $qc_outcomes_data_source->retrieve_seq_outcome($rp_key);
}

sub _get_product_mqc_outcomes {
  my ($self, $digests) = @_;

  my $qc_outcomes_data_source = npg_warehouse::loader::illumina::fqc->new(
    schema_qc => $self->schema_qc, digests => $digests
  );
  $qc_outcomes_data_source->retrieve();
  my %outcomes = map { $_ => $qc_outcomes_data_source->retrieve_outcomes($_) }
                 keys %{$digests};

  return \%outcomes;
}

sub _get_autoqc_results {
  my $self = shift;

  my $data = {};

  my $retriever = npg_qc::autoqc::qc_store->new(
    use_db      => 1,
    verbose     => $self->verbose,
    qc_schema   => $self->schema_qc,
    checks_list => [ qw/tag_metrics qX_yield/ ]
  );
  my $query = npg_qc::autoqc::qc_store::query->new(
    npg_tracking_schema => $self->schema_npg,
    id_run              => $self->id_run,
    option              => $ALLALL
  );
  my $collection = $retriever->load($query);
  return $data if $collection->is_empty();

  my $tag_metrucs_result_c = $collection->search({class_name => 'tag_metrics'});

  if (!$tag_metrucs_result_c->is_empty) {

    my $tag_metrucs_result = $tag_metrucs_result_c->pop();

    # Run-level data.
    $data->{run}->{tags_decode_percent} =
      $tag_metrucs_result->perfect_matches_percent;
    $data->{run}->{num_reads} =
      $tag_metrucs_result->total_reads_count('reads_pf_count');
    $data->{run}->{input_num_reads} =
      $tag_metrucs_result->total_reads_count('reads_count');

    # Sample-level data, including tag zero and control.
    foreach my $tag_index (keys %{$tag_metrucs_result->tags()}) {
      $data->{samples}->{$tag_index}->{tag_decode_count} =
        $tag_metrucs_result->perfect_matches_pf_count->{$tag_index};
      $data->{samples}->{$tag_index}->{tag_decode_percent} =
        $tag_metrucs_result->matches_pf_percent->{$tag_index};
      $data->{samples}->{$tag_index}->{is_sequencing_control} =
        ($tag_index == $tag_metrucs_result->spiked_control_index) ? 1 : 0;
      if ($tag_index) { # Not tag zero.
        $data->{samples}->{$tag_index}->{ultimagen_index_sequence} =
          $tag_metrucs_result->tags()->{$tag_index};
      }
    }
  }

  # And qX results for target samples and control.
  foreach my $result ($collection->all()) {
    next if ($result->class_name eq 'tag_metrics');
    my $tag_index = $result->tag_index;
    $data->{samples}->{$tag_index}->{q20_yield_kb} = $result->yield1_q20;
    $data->{samples}->{$tag_index}->{q30_yield_kb} = $result->yield1_q30;
    $data->{samples}->{$tag_index}->{total_yield_kb} = $result->yield1_total;
    my $digest = $result->composition->digest();
    $data->{samples}->{$tag_index}->{id_useq_product} = $digest;
    $data->{digests}->{$digest} = $result->composition;
  }

  return $data;
}

sub _link2lims {
  my ($self, $id_wafer_lims) = @_;

  $id_wafer_lims or croak 'LIMS identifier is needed';
  # Do we have any LIMS data for this wafer?
  my $uw_rs = $self->schema_wh->resultset('UseqWafer')
     ->search({id_wafer_lims => $id_wafer_lims});
  my $num_uf_rows = $uw_rs->count();
  if ($num_uf_rows == 0) {
    # No LIMS data. Normal for walk-up runs.
    $self->logger()->warn("No LIMS data for LIMS wafer ID '$id_wafer_lims'");
    return 0;
  }

  my %lims_data = map { $_->tag_sequence => $_->id_useq_wafer_tmp } $uw_rs->all();

  # Pick up rows for deplexed data to link.
  my $pr_rs = $self->schema_wh->resultset('UseqProductMetric')->search({
    id_run => $self->id_run,
    ultimagen_index_sequence => { q[!=] => $UNASSIGNED_DATA_TAG_INDEX },
    is_sequencing_control => { q[!=] => 1 },
  });

  # Establish mapping between the rows in useq_wafer and useq_product_metrics
  # rows. If the mapping is partial, link whatever maps and warn.
  my $num2link = $pr_rs->count();
  while (my $pr_row = $pr_rs->next) {
    my $uis = $pr_row->ultimagen_index_sequence;
    if (defined $uis) {
      my $uw_id = delete $lims_data{$uis};
      if (defined $uw_id) {
        $pr_row->update({'id_useq_wafer_tmp' => $uw_id});
      }
    }
  }

  my $num_unlinked = scalar keys %lims_data;
  if ($num_unlinked) {
    if ($num_unlinked == $num2link) {
      $self->logger()->warn(sprintf
        'Tag mismatch, no products for run %i is linked to LIMS data',
        $self->id_run
      );
    } else {
      $self->logger()->warn(sprintf
        '%i products for run %i have not been linked to LIMS data',
        $num_unlinked, $self->id_run
      );
    }
  }

  return ($num2link - $num_unlinked);
}

sub _generate_composition {
  my ($self, $tag_index) = @_;
  return npg_tracking::glossary::composition->new(components => [
    npg_tracking::glossary::composition::component::illumina->new(
      id_run => $self->id_run, position => $LANE, tag_index => $tag_index
    )
  ]);
}

__PACKAGE__->meta->make_immutable;

1;

__END__

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Readonly

=item Carp

=item Try::Tiny

=item Moose

=item MooseX::StrictConstructor

=item MooseX::Getopt

=item npg_warehouse::loader::illumina::fqc

=item npg_warehouse::loader::illumina::npg

=item npg_warehouse::loader::base

=item npg_tracking::glossary::run

=item npg_tracking::glossary::composition

=item npg_tracking::glossary::composition::component::illumina

=item npg_qc::ultimagen::sample_retriever

=item npg_qc::autoqc::qc_store

=item npg_qc::autoqc::qc_store::options

=item use npg_qc::autoqc::qc_store::query

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Marina Gourtovaia

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2026 Genome Research Ltd.

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
