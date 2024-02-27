package npg_warehouse::loader::pacbio::product;

use English qw[-no_match_vars];
use JSON;
use Moose::Role;
use Perl6::Slurp;
use Readonly;
use WTSI::DNAP::Warehouse::Schema::Result::PacBioProductMetric;

with 'npg_warehouse::loader::pacbio::base';

our $VERSION = '0';

Readonly::Scalar my $PRODUCT_TABLE_NAME  => q[PacBioProductMetric];
Readonly::Scalar my $RUN_TABLE_NAME      => q[PacBioRun];
Readonly::Scalar my $RUN_WELL_TABLE_NAME => q[PacBioRunWellMetric];
Readonly::Scalar my $ID_SCRIPT           => q[generate_pac_bio_id];
Readonly::Scalar my $ID_LENGTH           => 64;
Readonly::Scalar my $BARCODE_REPORT_NAME => q[Report barcode];
Readonly::Scalar my $IDX_DEFAULT_NAME    => q[default--default];
Readonly::Scalar my $HUNDRED             => 100;


=head1 NAME

npg_warehouse::loader::pacbio::product

=head1 SYNOPSIS

=head1 DESCRIPTION

=head1 SUBROUTINES/METHODS

=head2 product_data

  Arg [1]    : Well data for a run, ArrayRef[HashRef].
  Example    : my $product_data = $self->product_data($well_data);
  Description: Fetch sample/tags for a run from the pac_bio_run table
               which correspond to a set of run wells from SMRT LINK.
  Returntype : ArrayRef

=cut

sub product_data {
  my ($self, $well_data) = @_;

  defined $well_data or
    $self->logconfess('A defined well data argument is required');

  my $rs  = $self->mlwh_schema->resultset($RUN_TABLE_NAME);

  my @product_data;
  if ($well_data) {
    foreach my $well (@{$well_data}) {
      my $run_name   = $well->{'pac_bio_run_name'};
      my $well_label = $well->{'well_label'};
      my $pn         = $well->{'plate_number'};
      my $query = { pac_bio_run_name => $run_name, well_label => $well_label, };
      if (defined $pn) { $query->{plate_number} = $pn; }
      my $pac_bio_run = $rs->search($query);

      my $bcdata;
      my $rt = $npg_warehouse::loader::pacbio::run::CCSREADS;
      if (defined $well->{'ccs_execution_mode'} &&
        $well->{'ccs_execution_mode'} eq 'OnInstrument' &&
        defined $well->{'sl_ccs_uuid'}) {
        $bcdata = $self->_bc_deplex_info($well->{'sl_ccs_uuid'},$rt);
      }

      # The value for id product in run_well_metrics is not reused so that
      # samples from the same well with different tags and deplexed reads can
      # be differentiated.
      while (my $row = $pac_bio_run->next) {
        my $tags     = $row->get_tags;
        my $sm_tname = $self->_make_sm_tname($row);

        my $id_product = $self->generate_product_id(
          $run_name, $well_label, $tags, $pn);

        my $product =
          { 'id_pac_bio_tmp'             => $row->id_pac_bio_tmp,
            'pac_bio_run_name'           => $run_name,
            'well_label'                 => $well_label,
            'plate_number'               => $pn,
            'id_pac_bio_product'         => $id_product,
          };

        foreach my $column_name
            (qw/ barcode_quality_score_mean hifi_read_bases hifi_num_reads hifi_read_length_mean/) {
          $product->{$column_name} = $self->_get_qual_metric($column_name, $well, $sm_tname, $bcdata);
        }

        if (defined $product->{'hifi_read_bases'} && $well->{'hifi_read_bases'}) {
          my $perc = sprintf '%.2f',
            (($product->{'hifi_read_bases'}/$well->{'hifi_read_bases'}) * $HUNDRED);
          $product->{'hifi_bases_percent'} = $perc;
        }

        push @product_data, $product;
      }
    }
  }
  return \@product_data;
}


=head2 load_pacbioproductmetric_table

  Arg [1]    : Table data, ArrayRef[HashRef].
  Example    : $count = $self->load_pacbioproductmetric_table($data);
  Description: Loads where run and well exists in both the pac_bio_run 
               and pac_bio_run_well_metrics tables and can be linked. 
  Returntype :

=cut

sub load_pacbioproductmetric_table {
  my ($self, $table_data) = @_;

  defined $table_data or
    $self->logconfess('A defined table data argument is required');

  my $transaction = sub {
    my $count = 0;
    my $rs = $self->mlwh_schema->resultset($PRODUCT_TABLE_NAME);
    foreach my $row (@{$table_data}) {
      my $run  = delete $row->{'pac_bio_run_name'};
      my $well = delete $row->{'well_label'};
      my $pn   = delete $row->{'plate_number'};

      my ($fk) = $self->_get_run_well_fk($run,$well,$pn);

      if ($fk) {
        $row->{'id_pac_bio_rw_metrics_tmp'} = $fk;

        $self->info(q[Will update or create record in] .
          qq[ $PRODUCT_TABLE_NAME for run $run, well $well]);

        $rs->update_or_create($row, {key=> 'pac_bio_metrics_product'});
        $count++;
      }
    }
    return $count;
  };
  return $self->mlwh_schema->txn_do($transaction);
}

=head2 generate_product_id

  Arg [1]    : Run name, String. Required.
  Arg [2]    : Well label, String. Required.
  Arg [3]    : Comma separated list of tag sequences, String. Optional.
  Arg [4]    : Plate number. Integer. Optional.
  Example    : $id = $self->generate_product_id($run, $well, $tags);
  Description: Runs a python script which generates a product id from run,
               well and tag data.

=cut

sub generate_product_id {
  my ($self, $run_name, $well_label, $tags, $pn) = @_;

  my $command = join q[ ],
    $ID_SCRIPT, '--run_name', $run_name, '--well_label', $well_label;
  foreach my $tag (@{$tags}){
    $command .= join q[ ], ' --tag', $tag;
  }
  if (defined $pn) { $command .= ' --plate_number '. $pn; }

  $self->info("Generating product id: $command");
  open my $id_product_script, q[-|], $command
    or $self->logconfess('Cannot generate id_product ' . $CHILD_ERROR);
  my $id_product = <$id_product_script>;
  close $id_product_script
    or $self->logconfess('Could not close id_product generation script');
  $id_product =~ s/\s//xms;
  if (length $id_product != $ID_LENGTH) {
    $self->logcroak('Incorrect output length from id_product generation script, expected a 64 character string');
  }
  return $id_product;
}

sub _get_run_well_fk {
  my ($self, $run, $well, $plate_number) = @_;

  my $rs = $self->mlwh_schema->resultset($RUN_WELL_TABLE_NAME);
  my $query = { pac_bio_run_name => $run, well_label => $well, };
  if (defined $plate_number) { $query->{plate_number} = $plate_number; }

  my $pbrwm = $rs->search($query);

  my $fk = $pbrwm->count == 1 ? $pbrwm->first->id_pac_bio_rw_metrics_tmp : q[];
  return $fk;
}

sub _bc_deplex_info {
  my ($self, $id, $type) = @_;

  my $reports  = $self->pb_api_client->query_dataset_reports($type, $id);

  my $decoded;
  foreach my $rep(@{$reports}) {
    next if $rep->{dataStoreFile}->{name} ne $BARCODE_REPORT_NAME;
    if ($rep->{dataStoreFile}->{path} && -f $rep->{dataStoreFile}->{path}) {
      my $file_contents = slurp $rep->{dataStoreFile}->{path};
      $decoded = decode_json($file_contents);
    }
  }

  my %binfo; # data stored by barcode
  my %dinfo; # data hash to return

  my %header = (
    'Barcode Quality' => 'barcode_quality_score_mean',
    'HiFi Yield (bp)' => 'hifi_read_bases',
    'HiFi Reads'      => 'hifi_num_reads',
    'HiFi Read Length (mean, bp)' => 'hifi_read_length_mean',
  );

  if (defined $decoded && defined $decoded->{'tables'} ) {
    foreach my $table ( @{$decoded->{'tables'}} ) {
      foreach my $column ( @{$table->{'columns'}} ) {
        if (defined $column->{'values'} && defined $column->{'values'}->[0]) {
          if($column->{'header'} eq 'Barcode'){
            my $count = 0;
            %binfo = map { $count++ => $_ } @{$column->{'values'}};
          }
        }
      }
      foreach my $column ( @{$table->{'columns'}} ) {
        if (defined $column->{'values'} && defined $column->{'values'}->[0] &&
          defined $header{$column->{'header'}}) {
          my $c = 0;
          my %temp = map { $c++ => $_ } @{$column->{'values'}};
          foreach my $t(keys %temp){
            $dinfo{$binfo{$t}}{$header{$column->{'header'}}} = $temp{$t};
          }
        }
      }
    }
  }
  return \%dinfo;
}

sub _make_sm_tname {
  my($self, $row) = @_;

  my $tname; ## the tag identifier used in SMRT Link
  if(defined $row->tag_identifier && defined $row->tag2_identifier) {
    $tname = $row->tag_identifier .q[--]. $row->tag2_identifier;
  } elsif (defined $row->tag_identifier) {
    $tname = $row->tag_identifier .q[--]. $row->tag_identifier;
  }
  return($tname);
}

sub _get_qual_metric {
  my($self, $type, $well, $sm_tname, $bcdata) = @_;

  my $value;
  if ( defined $sm_tname && $bcdata->{$sm_tname}{$type} ) {
    $value = $bcdata->{$sm_tname}{$type};
  } elsif ( ! defined $sm_tname ) {
    ## check for default barcode results and if
    ## not present fall back to cell level results
    if ( $bcdata->{$IDX_DEFAULT_NAME}{$type} ) {
      $value = $bcdata->{$IDX_DEFAULT_NAME}{$type};
    } elsif ( defined $well->{$type} ) {
      $value = $well->{$type};
    }
  }

  return $value;
}

no Moose::Role;

1;

__END__

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item English

=item JSON

=item Moose::Role

=item Perl6::Slurp

=item Readonly

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
