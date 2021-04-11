package npg_warehouse::loader::pacbio::run;

use namespace::autoclean;
use English qw[-no_match_vars];
use JSON;
use Moose;
use MooseX::StrictConstructor;
use Perl6::Slurp;
use Readonly;
use Try::Tiny;

with qw[npg_warehouse::loader::pacbio::base
        npg_warehouse::loader::pacbio::product
        WTSI::DNAP::Utilities::Loggable
       ];

our $VERSION = '0';

Readonly::Scalar my $RUN_WELL_TABLE_NAME => q[PacBioRunWellMetric];
Readonly::Scalar my $PRODUCT_TABLE_NAME  => q[PacBioProductMetric];


=head1 NAME

npg_warehouse::loader::pacbio::run

=head1 SYNOPSIS

 npg_warehouse::loader::pacbio::run->new(@args)->load_run;

=head1 DESCRIPTION

=head1 SUBROUTINES/METHODS

=cut 

has 'run_uuid' =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 1,
   documentation => 'The PacBio run unique identifier',);

has '_run_name' =>
  (isa           => 'Str',
   is            => 'ro',
   lazy          => 1,
   builder       => q[_build_run_name],
   documentation => 'The shared Pacbio and SequenceScape/TRACTION run name',);

sub _build_run_name {
  my $self = shift;

  my $run = $self->_run;
  return $run->{'pac_bio_run_name'};
}

has '_run_data' =>
  (isa           => 'HashRef',
   is            => 'ro',
   lazy          => 1,
   builder       => '_build_run_data',
   documentation => 'Run, well and product data for a run',);

sub _build_run_data {
  my $self  = shift;

  my $well_data    = $self->_run_wells;
  my $product_data = $self->product_data($well_data);

  return { $RUN_WELL_TABLE_NAME  => $well_data,
           $PRODUCT_TABLE_NAME   => $product_data,
          };
}

has '_run' =>
  (isa           => 'HashRef',
   is            => 'ro',
   lazy          => 1,
   builder       => '_build_run',
   documentation => 'Fetch data for a run via the API',);

sub _build_run {
  my $self  = shift;
  my $run   = $self->pb_api_client->query_run($self->run_uuid);

  my %run_info;
  if (ref $run eq 'HASH') {
    $run_info{'pac_bio_run_name'}      = $run->{'name'};
    $run_info{'instrument_name'}       = $run->{'instrumentName'};
    $run_info{'instrument_type'}       = $run->{'instrumentType'};
    $run_info{'chip_type'}             = $run->{'chipType'};
    $run_info{'ts_run_name'}           = $run->{'context'};
    $run_info{'run_start'}             = $self->fix_date($run->{'startedAt'});
    $run_info{'run_complete'}          = $self->fix_date($run->{'completedAt'});
    $run_info{'run_status'}            = $run->{'status'};
    $run_info{'chemistry_sw_version'}  = $run->{'chemistrySwVersion'};
    $run_info{'instrument_sw_version'} = $run->{'instrumentSwVersion'};
    $run_info{'primary_analysis_sw_version'}
                                       = $run->{'primaryAnalysisSwVersion'};
  }
  return \%run_info;
}

has '_run_wells' =>
  (isa           => 'ArrayRef',
   is            => 'ro',
   lazy          => 1,
   builder       => '_build_run_wells',
   documentation => 'Fetch well data for a run via the API',);

sub _build_run_wells {
  my $self  = shift;
  my $wells =  $self->pb_api_client->query_run_collections($self->run_uuid);

  my @run_wells;
  if (ref $wells eq 'ARRAY') {
    foreach my $well (@{$wells}) {
      my %well_info;
      $well->{'well'} =~ s/0//smx;
      $well_info{'well_label'}         = $well->{'well'};
      $well_info{'movie_name'}         = $well->{'context'};
      $well_info{'well_start'}         = $self->fix_date($well->{'startedAt'});
      $well_info{'well_complete'}      = $self->fix_date($well->{'completedAt'});
      $well_info{'well_status'}        = $well->{'status'};
      $well_info{'ccs_execution_mode'} = $well->{'ccsExecutionMode'};

      my $qc = defined $well->{'ccsExecutionMode'} &&
        $well->{'ccsExecutionMode'} eq 'OnInstrument' ?
        $self->_well_qc_info($well->{'ccsId'}, q[ccsreads]) :
        $self->_well_qc_info($well->{'uniqueId'}, q[subreads]);

      my $run = $self->_run;
      my %all = (%{$run}, %well_info, %{$qc});
      push @run_wells, \%all;
    }
  }
  return \@run_wells;
}

sub _well_qc_info {
  my ($self, $id, $type) = @_;

  my $reports  = $self->pb_api_client->query_dataset_reports($type, $id);

  my %qc_all;
  foreach my $rep (@{$reports}) {
    # directly slurp in each file (consider server download in future)
    if ($rep->{dataStoreFile}->{path} && -f $rep->{dataStoreFile}->{path}) {
      my $file_contents = slurp $rep->{dataStoreFile}->{path};
      my $decoded       = decode_json($file_contents);
      if (defined $decoded->{'attributes'} ) {
        foreach my $att ( @{$decoded->{'attributes'}} ) {
          $qc_all{$att->{'name'}} = $att->{'value'};
        }
      }
    }
  }

  my %qc;
  if ( scalar keys %qc_all > 0 ) {
    $qc{'polymerase_read_bases'}       = $qc_all{'Polymerase Read Bases'};
    $qc{'polymerase_num_reads'}        = $qc_all{'Polymerase Reads'};
    $qc{'polymerase_read_length_mean'} = $qc_all{'Polymerase Read Length (mean)'};
    $qc{'polymerase_read_length_n50'}  = $qc_all{'Polymerase Read N50'};
    $qc{'insert_length_mean'}          = $qc_all{'Longest Subread Length (mean)'};
    $qc{'insert_length_n50'}           = $qc_all{'Longest Subread N50'};
    $qc{'unique_molecular_bases'}      = $qc_all{'Unique Molecular Yield'};
    $qc{'productive_zmws_num'}         = $qc_all{'Productive ZMWs'};
    $qc{'p0_num'}                      = $qc_all{'Productivity 0'};
    $qc{'p1_num'}                      = $qc_all{'Productivity 1'};
    $qc{'p2_num'}                      = $qc_all{'Productivity 2'};
    $qc{'adapter_dimer_percent'}       = $qc_all{'Adapter Dimers (0-10bp) %'};
    $qc{'short_insert_percent'}        = $qc_all{'Short Inserts (11-100bp) %'};
  }
  return \%qc;
}

=head2 load_pacbiorunwellmetric_table

  Arg [1]    : Table data, ArrayRef[HashRef].
  Example    : $count = $self->load_pacbiorunwellmetric_table($data);
  Description: Loads data for one run to the pac_bio_run_well_metrics table.
  Returntype : Int

=cut

sub load_pacbiorunwellmetric_table {
  my ($self, $table_data) = @_;

  defined $table_data or
    $self->logconfess('A defined table data argument is required');

  my $transaction = sub {
    my $count = 0;
    my $rs = $self->mlwh_schema->resultset($RUN_WELL_TABLE_NAME);
    foreach my $row (@{$table_data}) {
      $self->info(
        "Will update or create record in $RUN_WELL_TABLE_NAME for " .
        join q[ ], 'run', $row->{'pac_bio_run_name'}, 'well', $row->{'well_label'}
      );
      $rs->update_or_create($row);
      $count++;
    }
    return $count;
  };
  return $self->mlwh_schema->txn_do($transaction);
}

=head2 load_run

  Arg [1]    : None
  Example    : my ($processed, $loaded, $errors) = $loader->load_run;
  Description: Publish data for one run to the mlwarehouse. 
  Returntype : Array[Int]

=cut

sub load_run {
  my ($self) = @_;

  my ($num_processed, $num_loaded, $num_errors) = (0, 0, 0);

  my $run_name;
  try {
    $run_name = $self->_run_name;
  } catch {
    $self->error('Failed to find run uuid '. $self->run_uuid);
  };
  $run_name or return ($num_processed, $num_loaded, $num_errors);

  my $data;
  try {
    $data = $self->_run_data;
    $num_processed++;
  } catch {
    $self->error('Failed to process run '. $run_name .' cleanly ', $_);
  };

  my @tables = ($RUN_WELL_TABLE_NAME, $PRODUCT_TABLE_NAME);
  my %calls  = map { $_ => join q[_], q[load], lc $_, q[table] } @tables;

  if ($data && $num_errors < 1 && !$self->dry_run) {
    foreach my $table (@tables) {
      if (!defined $data->{$table} || scalar @{$data->{$table}} == 0) {
        $self->info(qq[Run $run_name - no data for table $table]);
      } else {
        my $count = 0;
        try {
          my $method_name = $calls{$table};
          $count = $self->$method_name($data->{$table});
          $self->info(qq[Loaded $count rows to table $table for run $run_name]);
        } catch {
          my $err = $_;
          ($err =~ /Rollback failed/sxm) and $self->logcroak($err);
          $self->logwarn(qq[Failed to load run $run_name : $err]);
          $num_errors++;
        };
        $count or last;
      }
    }
    if ($num_errors < 1) { $num_loaded++; }
  }
  return ($num_processed, $num_loaded, $num_errors);
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item namespace::autoclean

=item English

=item JSON

=item Moose

=item MooseX::StrictConstructor

=item Perl6::Slurp

=item Readonly

=item Try::Tiny

=item npg_warehouse::loader::pacbio::base

=item npg_warehouse::loader::pacbio::product

=item WTSI::DNAP::Utilities::Loggable

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2021 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
