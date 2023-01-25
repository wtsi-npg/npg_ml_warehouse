package npg_warehouse::loader::pacbio::run;

use namespace::autoclean;
use English qw[-no_match_vars];
use JSON;
use Moose;
use MooseX::StrictConstructor;
use Perl6::Slurp;
use Readonly;
use Try::Tiny;
use XML::LibXML;

with qw[npg_warehouse::loader::pacbio::base
        npg_warehouse::loader::pacbio::product
        WTSI::DNAP::Utilities::Loggable
       ];

our $VERSION = '0';

Readonly::Scalar my $RUN_WELL_TABLE_NAME  => q[PacBioRunWellMetric];
Readonly::Scalar my $PRODUCT_TABLE_NAME   => q[PacBioProductMetric];
Readonly::Scalar my $MIN_SW_VERSION       => 10;
Readonly::Scalar my $XML_TRACKING_FIELDS  => 6;
Readonly::Scalar my $XML_TRACKING_FIELDS2 => 8;
Readonly::Scalar my $SUBREADS             => q[subreads];
Readonly::Scalar my $CCSREADS             => q[ccsreads];

=head1 NAME

npg_warehouse::loader::pacbio::run

=head1 SYNOPSIS

 npg_warehouse::loader::pacbio::run->new(@args)->load_run;

=head1 DESCRIPTION

=head1 SUBROUTINES/METHODS

=cut 

has 'hostname' =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 1,
   documentation => 'The SMRT Link server hostname e.g. pacbio02.dnapipelines.sanger.ac.uk',);

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

has '_run_sw_version' =>
  (isa           => 'Str',
   is            => 'ro',
   lazy          => 1,
   builder       => q[_build_run_sw_version],
   documentation => 'The run primary analysis software version',);

sub _build_run_sw_version {
  my $self = shift;

  my $run = $self->_run;
  return $run->{'primary_analysis_sw_version'};
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
    $run_info{'sl_hostname'}           = $self->hostname;
    $run_info{'sl_run_uuid'}           = $run->{'uniqueId'};
    $run_info{'ts_run_name'}           = $run->{'context'};
    $run_info{'run_start'}             = $self->fix_date($run->{'startedAt'});
    $run_info{'run_complete'}          = $self->fix_date($run->{'completedAt'});
    $run_info{'run_status'}            = $run->{'status'};
    $run_info{'run_transfer_complete' } = $self->fix_date($run->{'transfersCompletedAt'});
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
      $well_info{'movie_minutes'}      = $well->{'movieMinutes'};
      $well_info{'well_start'}         = $self->fix_date($well->{'startedAt'});
      $well_info{'well_complete'}      = $self->fix_date($well->{'completedAt'});
      $well_info{'well_status'}        = $well->{'status'};
      $well_info{'ccs_execution_mode'} = $well->{'ccsExecutionMode'};
      $well_info{'created_by'}         = $well->{'createdBy'};

      my $qc = defined $well->{'ccsExecutionMode'} &&
        $well->{'ccsExecutionMode'} eq 'OnInstrument' ?
        $self->_well_qc_info($well->{'ccsId'}, $CCSREADS) :
        $self->_well_qc_info($well->{'uniqueId'}, $SUBREADS);

      my $ccs;
      if (defined $well->{'ccsId'}) {
        $ccs = $self->_ccs_info($well->{'ccsId'}, $CCSREADS);
      }

      if (scalar keys %{$ccs} == 0 && defined $well->{'context'}) {
        ## find non linked ccs data e.g. off instrument initial ccs job failed data
        my ($ccsid) = $self->_sift_for_dataset($well->{'name'}, $well->{'context'});
        if (defined $ccsid) { $ccs = $self->_ccs_info($ccsid, $CCSREADS); }
      }

      my $tki = $self->_well_tracking_info($well->{'uniqueId'});
      my $run = $self->_run;

      my %all =  (%well_info, %{$qc}, %{$ccs}, %{$tki}, %{$run});
      push @run_wells, \%all;
    }
  }
  return \@run_wells;
}

has '_ccs_datasets' =>
  (isa           => 'ArrayRef',
   is            => 'ro',
   lazy          => 1,
   builder       => q[_build_ccs_datasets],
   documentation => 'Fetch ccs datasets from the API',);

sub _build_ccs_datasets {
  my $self = shift;
  return $self->pb_api_client->query_datasets($CCSREADS);
}

sub _sift_for_dataset {
  my ($self, $well_name, $movie_name) = @_;

  defined $well_name or
    $self->logconfess('A defined well_name argument is required');

  defined $movie_name or
    $self->logconfess('A defined movie_name argument is required');

  my $datasets = $self->_ccs_datasets();

  my @ccsid;
  foreach my $set (@{$datasets}) {
    if ($set->{'name'} =~ /^$well_name/smx &&
        $set->{'name'} =~ /[(] CCS [)] $/smx &&
        $set->{'metadataContextId'} eq $movie_name) {
      push @ccsid, $set->{'uuid'};
    }
  }
  return (scalar @ccsid == 1) ? $ccsid[0] : undef;
}

sub _ccs_info {
  my ($self, $id, $type) = @_;

  my $reports  = $self->pb_api_client->query_dataset_reports($type, $id);
  my $ccs_all  = $self->_slurp_reports($reports);

  my %ccs;
  if (scalar keys %{$ccs_all} > 0 && defined $ccs_all->{'HiFi Yield (bp)'}) {
    $ccs{'hifi_read_bases'} = $ccs_all->{'HiFi Yield (bp)'};
    $ccs{'hifi_num_reads'} = $ccs_all->{'HiFi Reads'};
    $ccs{'hifi_read_length_mean'} = $ccs_all->{'HiFi Read Length (mean, bp)'};
    if ($ccs_all->{'HiFi Read Quality (median)'} =~ /^Q?(\d+)$/smx) {
      $ccs{'hifi_read_quality_median'} = $1;
    }
    $ccs{'hifi_number_passes_mean'} = $ccs_all->{'HiFi Number of Passes (mean)'};
    $ccs{'hifi_low_quality_read_bases'} = $ccs_all->{'<Q20 Yield (bp)'};
    $ccs{'hifi_low_quality_num_reads'} = $ccs_all->{'<Q20 Reads'};
    $ccs{'hifi_low_quality_read_length_mean'}  = $ccs_all->{'<Q20 Read Length (mean, bp)'};
    if ($ccs_all->{'<Q20 Read Quality (median)'} &&
        $ccs_all->{'<Q20 Read Quality (median)'} =~ /^Q?(\d+)$/smx) {
      $ccs{'hifi_low_quality_read_quality_median'} = $1;
    }
  }
  return \%ccs;
}

sub _well_qc_info {
  my ($self, $id, $type) = @_;

  my $reports = $self->pb_api_client->query_dataset_reports($type, $id);
  my $qc_all  = $self->_slurp_reports($reports);

  my %qc;
  if (scalar keys %{$qc_all} > 0 && defined $qc_all->{'Polymerase Reads'}) {
    $qc{'polymerase_read_bases'}       = $qc_all->{'Polymerase Read Bases'};
    $qc{'polymerase_num_reads'}        = $qc_all->{'Polymerase Reads'};
    $qc{'polymerase_read_length_mean'} = $qc_all->{'Polymerase Read Length (mean)'};
    $qc{'polymerase_read_length_n50'}  = $qc_all->{'Polymerase Read N50'};
    $qc{'insert_length_mean'}          = $qc_all->{'Longest Subread Length (mean)'};
    $qc{'insert_length_n50'}           = $qc_all->{'Longest Subread N50'};
    $qc{'unique_molecular_bases'}      = $qc_all->{'Unique Molecular Yield'};
    $qc{'productive_zmws_num'}         = $qc_all->{'Productive ZMWs'};
    $qc{'p0_num'}                      = $qc_all->{'Productivity 0'};
    $qc{'p1_num'}                      = $qc_all->{'Productivity 1'};
    $qc{'p2_num'}                      = $qc_all->{'Productivity 2'};
    $qc{'adapter_dimer_percent'}       = $qc_all->{'Adapter Dimers (0-10bp) %'};
    $qc{'short_insert_percent'}        = $qc_all->{'Short Inserts (11-100bp) %'};
    ## field name corrected in SMRT Link v11 - but require both for historic data
    $qc{'control_num_reads'}           = defined $qc_all->{'Number of of Control Reads'} ?
      $qc_all->{'Number of of Control Reads'} : $qc_all->{'Number of Control Reads'};
    $qc{'control_concordance_mean'}    = $qc_all->{'Control Read Concordance Mean'};
    $qc{'control_concordance_mode'}    = $qc_all->{'Control Read Concordance Mode'};
    $qc{'control_read_length_mean'}    = $qc_all->{'Control Read Length Mean'};
    $qc{'local_base_rate'}             = $qc_all->{'Local Base Rate'};
  }
  return \%qc;
}

sub _slurp_reports {
  my($self, $reports) = @_;

  defined $reports or
    $self->logconfess('A defined reports argument is required');

  my %contents;
  foreach my $rep (@{$reports}) {
    # directly slurp in each file (consider server download in future)
    if ($rep->{dataStoreFile}->{path} && -f $rep->{dataStoreFile}->{path}) {
      my $file_contents = slurp $rep->{dataStoreFile}->{path};
      my $decoded       = decode_json($file_contents);
      if (defined $decoded->{'attributes'} ) {
        foreach my $att ( @{$decoded->{'attributes'}} ) {
          $contents{$att->{'name'}} = $att->{'value'};
        }
      }
    }
  }
  return \%contents;
}

sub _well_tracking_info {
  my ($self, $id) = @_;

  my $run = $self->pb_api_client->query_run($self->run_uuid);

  my (%well_tracking_info, $tracking);
  my $errors = 0;

  if ($run->{'dataModel'} ) {
    try {
      my $dom    =  XML::LibXML->load_xml(string => $run->{'dataModel'});
      my $prefix = q[];

      my @subsets;
      if (defined $dom->getElementsByTagName('pbds:SubreadSet')->[0]) {
          @subsets = $dom->getElementsByTagName('pbds:SubreadSet');
          $prefix = q[pbmeta:];
      } elsif (defined $dom->getElementsByTagName('SubreadSet')->[0]) {
          @subsets = $dom->getElementsByTagName('SubreadSet');
      }

      foreach my $sub ( @subsets ) {
        my $idx   = $sub->getAttribute('UniqueId');
        next if $id ne $idx;

        $tracking = $self->_well_tracking_subset_parser($prefix, $sub);
      }
    } catch {
      $self->error('Errors getting info for ', $run->{'name'},' subset ', $id, $_);
      $errors++;
    };

    if ($errors < 1 && defined $tracking) {
      %well_tracking_info = %{$tracking};
    }

    if ((scalar keys %well_tracking_info != $XML_TRACKING_FIELDS) &&
        (scalar keys %well_tracking_info != $XML_TRACKING_FIELDS2) ) {
      $self->error('Failed to get all expected XML info for ', $run->{'name'},' subset ', $id);
    }
  }
  return \%well_tracking_info;
}

sub _well_tracking_subset_parser {
  my ($self, $prefix, $sub) = @_;

  my %tracking;
  if ($sub->getElementsByTagName($prefix .'OnPlateLoadingConcentration')) {
    my $conc = $sub->getElementsByTagName($prefix .'OnPlateLoadingConcentration');
    $conc =~ s/[.]0*$//smx;
    $tracking{'loading_conc'} = $conc;
  }
  if ($sub->getElementsByTagName($prefix .'IncludeKinetics')) {
    my $kin  = $sub->getElementsByTagName($prefix .'IncludeKinetics') eq 'true' ? 1 : 0;
    $tracking{'include_kinetics'} = $kin;
  }
  if ($sub->getElementsByTagName($prefix .'AutomationParameters')) {
    my $hifio = $self->_well_tracking_automation_parser($prefix,$sub,'AllReads');
    if ( defined $hifio ) {
      $hifio eq 'true' ?
        ($tracking{'hifi_only_reads'} = 0) : ($tracking{'hifi_only_reads'} = 1);
    }
    my $hda = $self->_well_tracking_automation_parser($prefix, $sub,'Heteroduplex');
    if ( defined $hda ) {
      $hda eq 'true' ?
        ($tracking{'heteroduplex_analysis'} = 1) : ($tracking{'heteroduplex_analysis'} = 0);
    }
  }
  if ($sub->getElementsByTagName($prefix .'SequencingKitPlate')) {
    my $skp  = $sub->getElementsByTagName($prefix .'SequencingKitPlate')->[0];
    if ($skp->getAttribute('Name')) {
      my $skn = $skp->getAttribute('Name');
      $skn =~ s/[^[:ascii:]]//smxg;
      $tracking{'sequencing_kit'} = $skn;
    }
    if ($skp->getAttribute('LotNumber')) {
      $tracking{'sequencing_kit_lot_number'} = $skp->getAttribute('LotNumber');
    }
  }
  if ($sub->getElementsByTagName($prefix .'BindingKit')) {
    my $bk   = $sub->getElementsByTagName($prefix .'BindingKit')->[0];
    if ($bk->getAttribute('Name')) {
      my $bkn = $bk->getAttribute('Name');
      $bkn =~ s/[^[:ascii:]]//smxg;
      $tracking{'binding_kit'} = $bkn;
    }
  }
  if ($sub->getElementsByTagName($prefix .'CellPac')) {
    my $cell = $sub->getElementsByTagName($prefix .'CellPac')->[0];
    if ($cell->getAttribute('LotNumber')) {
      $tracking{'cell_lot_number'} = $cell->getAttribute('LotNumber');
    }
  }
  return \%tracking;
}

sub _well_tracking_automation_parser {
  my ($self, $prefix, $params, $tag) = @_;

  my $value;
  foreach my $e (@{$params->getElementsByTagName($prefix .'AutomationParameters')}){
    if ($e->getElementsByTagName($prefix .'AutomationParameter')) {
      my $ae = $e->getElementsByTagName($prefix .'AutomationParameter');
      foreach my $a (@{$ae}){
        if($a->getAttribute('Name') eq $tag){
          $value = $a->getAttribute('SimpleValue');
        }
      }
    }
  }
  return $value;
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

      my $run_name = $row->{'pac_bio_run_name'};
      my $well_label = $row->{'well_label'};
      my $message = "run $run_name well $well_label";

      my $db_row = $rs->find({
        pac_bio_run_name => $run_name, well_label => $well_label
      });
      if ($db_row) {
        $self->info("Will update record in $RUN_WELL_TABLE_NAME for $message");
        $db_row->update($row);
        my $products_rs = $db_row->pac_bio_product_metrics();
        my $unlinked = 0;
        while (my $product_db_row = $products_rs->next()) {
          if (not $product_db_row->id_pac_bio_tmp) {
            $unlinked = 1;
            last;
          }
        }
        if ($unlinked) {
          $self->warn("Found unlinked product rows for $message");
          $self->warn("Will delete all product rows for $message");
          $products_rs->delete();
        }
      } else {
        $self->info("Will create record in $RUN_WELL_TABLE_NAME for $message");
        $row->{'id_pac_bio_product'} =
          $self->generate_product_id($run_name, $well_label);
        $rs->create($row);
      }

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

  my ($run_name, $version);
  try {
    $run_name = $self->_run_name;
    my @swv   = split /[.]/smx, $self->_run_sw_version;
    $version  = $swv[0];
  } catch {
    $self->error('Failed to find basic info for run uuid '. $self->run_uuid);
  };

  ($run_name && $version && ($version >= $MIN_SW_VERSION)) or
    return ($num_processed, $num_loaded, $num_errors);

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

=item XML::LibXML

=item npg_warehouse::loader::pacbio::base

=item npg_warehouse::loader::pacbio::product

=item WTSI::DNAP::Utilities::Loggable

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2021, 2022, 2023 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
