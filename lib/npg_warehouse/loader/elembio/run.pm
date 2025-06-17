package npg_warehouse::loader::elembio::run;

use Moose;
use MooseX::StrictConstructor;
use Carp;

use npg_tracking::glossary::rpt;
use npg_tracking::illumina::run::folder;
use npg_tracking::Schema;
use npg_qc::Schema;
use WTSI::DNAP::Warehouse::Schema;
use npg_warehouse::loader::illumina::npg;
use npg_warehouse::loader::illumina::fqc;

with qw/ MooseX::Getopt
         npg_tracking::illumina::run::folder /;

our $VERSION = '0';

=head1 NAME

npg_warehouse::loader::elembio::run

=head1 SYNOPSIS
 
  my $path = 'some/path';
  npg_warehouse::loader::elembio::run->new(runfolder_path => $path)->load();

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

Run ID as recorded in NPG trackign database. Required.

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

# Always require trackign db connection.
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

The loader does not load product data for cancelled or stopped early runs.
Runs marked as stopped early will either be analysed and picked up by
the mlwh loader that runs within the analysis pipeline or manually moved to
'run calcelled' status. See C<npg_warehouse::loader::illumina::npg::run_is_cancelled>
for the definition of a cancelled run.

If LIMS batch ID value is set in the tracking database for this run,
an attempt to link eseq_product_metrics table to eseq_flowcell table is
made.

=cut

sub load {
  my $self = shift;

  -d $self->runfolder_path or croak
    sprintf 'Run folder path %s does not exist', $self->runfolder_path;

  my $rl_rs = $self->mlwh_schema->resultset('EseqRunLaneMetric');
  my %column_names = map { $_ => 1 } $rl_rs->result_source->columns;
  my $lane_data = $self->_get_run_lane_data(\%column_names);

  my $loader = sub {
    for my $data (@{$lane_data}) {
      $rl_rs->update_or_create($data);
    }

    if ($lane_data->[0]->{cancelled} ||
      $self->tracking_run->current_run_status_description() eq 'run stopped early') {
      return;
    }
    #TODO - load product data
  };
  $self->mlwh_schema->txn_do($loader);

  # If we have LIMS reference for a run, we will try to link eseq_product_metrics
  # run data to relevant records in eseq_flowcell table or relink if the foreign
  # keys have been already set.
  # my $batch_id = $self->tracking_run->batch_id;
  my $batch_id = 0;
  if ($batch_id) {
    $self->mlwh_schema->txn_do(
      \&{ $self->_link2lims->($batch_id) }
    );
  } else {
    carp 'No batch ID, cannot link product data for run folder ' .
      $self->runfolder_path . ' run ID ' . $self->id_run;
  }

  return;
}

sub _get_run_lane_data {
  my ($self, $column_names) = @_;

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
    $data{tags_decode_percent} = undef; #TODO
    $data{num_polonies} = undef; #TODO 
    for my $column_name ( keys %data ) {
      exists $column_names->{$column_name} or delete $data{$column_name};
    }
    push @per_lane_data, \%data;
  }

  return \@per_lane_data;
}

sub _link2lims {
  my ($self, $batch_id) = @_;

  # Do we have any LIMS data for this batch?
  my $fc_rs = $self->mlwh_schema->resultset('EseqFlowcell')
     ->search({if_flowcell_lims => $batch_id});
  my $num_fc_rows = $fc_rs->count();
  if ($num_fc_rows == 0) {
    carp "No LIMS data for batch ID $batch_id"; # Normal for walk-up runs.
    return;
  }

  my $na = q[NA];
  my $done = q[done];
  my $lims_data = {};
  while (my $fc_row = $fc_rs->next) {
    my $tag2 = $fc_row->tag2_sequence;
    $tag2 ||= $na;
    $lims_data->{$fc_row->lane}->{$fc_row->tag_sequence}->{$tag2} =
      $fc_row->id_eseq_flowcell_tmp;
  }

  # Pick up rows to link.
  my $id_run = $self->id_run;
  my $pr_rs = $self->mlwh_schema->resultset('EseqProductMetric')->search({
    id_run => $id_run,
    position => { q[!=] => undef},
    tag_sequence => { q[!=] => undef},
  });

  # Establish mapping between the rows in eseq_flowcell and eseq_product_metrics
  # rows. If the mapping is partial, link whatever maps and warn.
  my $num2link = $pr_rs->count();
  my $original_num2link = $num2link;
  while (my $pr_row = $fc_rs->next) {
    my $tag2 = $pr_row->tag2_sequence;
    $tag2 ||= $na;
    my $fc_id = $lims_data->{$pr_row->lane}->{$pr_row->tag_sequence}->{$tag2};
    if ($fc_id) {
      if ( $fc_id eq $done ) {
        croak 'Should not have this';
      }
      $num2link--;
      $pr_row->update(id_eseq_flowcell_tmp => $fc_id);
      $lims_data->{$pr_row->lane}->{$pr_row->tag_sequence}->{$tag2} = $done;
    }
  }
  if ($num2link) {
    if ($num2link == $original_num2link) {
      carp "No products for run $id_run is linked to LIMS data";
    } else {
      # OK for controls?
      carp "$num2link products for run $id_run have not been linked to LIMS data";
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

=item npg_tracking::glossary::rpt

=item WTSI::DNAP::Warehouse::Schema

=item npg_tracking::illumina::run::folder

=item npg_tracking::Schema

=item npg_qc::Schema

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
