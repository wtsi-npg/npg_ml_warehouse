package npg_warehouse::loader::run;

use Moose;
use MooseX::StrictConstructor;
use Try::Tiny;
use List::MoreUtils qw/ any /;
use Readonly;
use Carp;

use WTSI:DNAP::Warehouse::Schema;
use npg_tracking::Schema;
use npg_qc::Schema;
use npg_qc::autoqc::qc_store;

use npg_warehouse::loader::autoqc;
use npg_warehouse::loader::qc;
use npg_warehouse::loader::npg;

with 'npg_tracking::glossary::run';

our $VERSION  = '0';

Readonly::Scalar our $FORWARD_END_INDEX   => 1;
Readonly::Scalar our $REVERSE_END_INDEX   => 2;
Readonly::Scalar our $PLEXES_KEY         => q[plexes];

=head1 NAME

npg_warehouse::loader::run

=head1 SYNOPSIS

 npg::warehouse::loader::run->new(id_run => 4444)->load;

=head1 DESCRIPTION

=head1 SUBROUTINES/METHODS

=head2 id_run

Run id

=head2 verbose

Verbose flag

=cut
has 'verbose'      => ( isa        => 'Bool',
                        is         => 'ro',
                        required   => 0,
                        default    => 0,
);

=head2 _schema_wh

DBIx schema object for the warehouse database

=cut
has '_schema_wh'  =>  ( isa        => 'WTSI:DNAP::Warehouse::Schema',
                        is         => 'ro',
                        required   => 0,
                        lazy_build => 1,
);
sub _build__schema_wh {
  my $self = shift;
  my $schema = WTSI:DNAP::Warehouse::Schema->connect();
  if($self->verbose) {
    carp q[Connected to the warehouse db, schema object ] . $schema;
  }
  return $schema;
}

has '_flowcell_table_fks' => ( isa        => 'HashRef',
                               is         => 'ro',
                               required   => 0,
                               lazy_build => 1,
);
sub _build__flowcell_table_fks {
  my $self = shift;
  my $lane = $self->_run_lane_rs->[0];
  my $lims_id = $lane->batch_id;
  my $query = {};
  if (!$lims_id) {
    $lims_id = $lane->flowcell_id;
    $query = {'flowcell_barcode' => $lims_id,};
  } else {
    $query = {'id_flowcell_lims' => $lims_id,};
  }

  my $fks = {};
  if ($lims_id) {
    my $rs = $self->_schema_wh->resultset($table)->search($query);
    while (my $row = $rs->next()) {
      my $entity_type = $row->entity_type;
      my $position    =  $row->position;
      if (exists $fks->{$position}->{$entity_type}->{_pt_key($position, $row->tag_index)}) {
        croak qq[Entry for $entity_type, $rpt_key already exists];
      }
      $fks->{$entity_type}->{$rpt_key} = $row->id_iseq_flowcell_tmp;
    }
  } else {
    if ($self->verbose) {
      warn q[Tracking database has no lims information for run ] . $self->id_run . qq[\n]; 
    }
  }

  if ($lims_id && (scalar keys %{$fks} == 0) & $self->verbose) {
    warn qq[No lims information for flowcell $lims_id];
  }

  return $fks;
}

has '_have_flowcell_table_fks' => ( isa        => 'Bool',
                                    is         => 'ro',
                                    required   => 0,
                                    lazy_build => 1,
);
sub _build_have_flowcell_table_fks => {
  my $self = shift;
  return scalar keys %{$self->_flowcell_table_fks};
} 

=head2 _schema_npg

DBIx schema object for the npg database

=cut
has '_schema_npg' =>  ( isa        => 'npg_tracking::Schema',
                        is         => 'ro',
                        required   => 0,
                        lazy_build => 1,
);
sub _build__schema_npg {
  my $self = shift;
  my $schema = npg_tracking::Schema->connect();
  if($self->verbose) {
    warn q[Connected to the npg db, schema object ] . $schema . qq[\n];
  }
  return $schema;
}

=head2 _schema_qc

DBIx schema object for the NPG QC database

=cut
has '_schema_qc' =>   ( isa        => 'npg_qc::Schema',
                        is         => 'ro',
                        required   => 0,
                        lazy_build => 1,
);
sub _build__schema_qc {
  my $self = shift;
  my $schema = npg_qc::Schema->connect();
  if($self->verbose) {
    warn q[Connected to the qc db, schema object ] . $schema . qq[\n];
  }
  return $schema;
}

=head2 _autoqc_store

A driver to retrieve autoqc objects. If DB storage is not available,
it will give no error, so no need to mock DB for this one in tests.
Just mock the staging area in your tests

=cut
has '_autoqc_store' =>    ( isa        => 'npg_qc::autoqc::qc_store',
                            is         => 'ro',
                            required   => 0,
                            lazy_build => 1,
);
sub _build__autoqc_store {
  my $self = shift;
  return npg_qc::autoqc::qc_store->new(use_db    => 1,
                                       verbose   => $self->verbose,
                                       qc_schema => $self->_schema_qc);
}

=head2 _run_lane_rs

Result set object for run lanes that have to be loaded

=cut
has '_run_lane_rs' =>     ( isa        => 'DBIx::Class::ResultSet',
                            is         => 'ro',
                            required   => 0,
                            lazy_build => 1,
);
sub _build__run_lane_rs {
  my $self = shift;
  my @all_rs = $self->_schema_npg->resultset('RunLane')->search(
    { q[me.id_run] => $self->id_run},
    {
      prefetch => q[run],
      order_by => [q[me.id_run], q[me.position]],
    },
  )->all;
  return \@all_rs;
}

has '_autoqc_data'   =>   ( isa        => 'HashRef',
                            is         => 'ro',
                            required   => 0,
                            lazy_build => 1,
);
sub _build_autoqc_data {
  my $self = shift;
  if (!$self->_run_is_cancelled) {
    return npg_warehouse::loader::autoqc->new(
      autoqc_store => $self->_autoqc_store,
      verbose => $self->verbose,
      plex_key => $PLEXES_KEY)->retrieve($selg->id_run, $self->_schema_npg);
  }
  return {};
}


has '_npg_data_retriever'   =>   ( isa        => 'npg_warehouse::loader::npg',
                                   is         => 'ro',
                                   required   => 0,
                                   lazy_build => 1,
);
sub _build__npg_data_retriever {
  my $self = shift;
  return npg_warehouse::loader::npg->new(schema_npg => $self->_schema_npg,
                                         verbose    => $self->verbose,
                                         id_run     => $self->id_run);
}

has '_run_is_cancelled'   =>   ( isa        => 'Bool',
                                 is         => 'ro',
                                 required   => 0,
                                 lazy_build => 1,
);
sub _build__run_is_cancelled {
  my $self = shift;
  return $self->_npg_data_retriever->run_is_cancelled();
}

has '_run_is_paired_read'   => ( isa        => 'Bool',
                                 is         => 'ro',
                                 required   => 0,
                                 lazy_build => 1,
);
sub _build__run_is_paired_read {
  my $self = shift;
  return $self->_npg_data_retriever->run_is_paired_read();
}

has '_npgqc_data_retriever'   => ( isa        => 'npg_warehouse::loader::qc',
                                   is         => 'ro',
                                   required   => 0,
                                   lazy_build => 1,
);
sub _build__npgqc_data_retriever {
  my $self = shift;
  return npg_warehouse::loader::qc->new(schema_npg        => $self->_schema_qc,
                                        verbose           => $self->verbose,
                                        reverse_end_index => $REVERSE_END_INDEX,
                                        plex_key          => $PLEXES_KEY);
}

has '_run_end_summary'   =>    ( isa        => 'HashRef',
                                 is         => 'ro',
                                 required   => 0,
                                 lazy_build => 1,
);
sub _build__run_end_summary {
  my $self = shift;
  return $self->_npgqc_data_retriever->retrieve_summary(
    $self->id_run, $FORWARD_END_INDEX, $self->_run_lane_rs->[0]->run->is_paired);
}

has '_qyields'           =>    ( isa        => 'HashRef',
                                 is         => 'ro',
                                 required   => 0,
                                 lazy_build => 1,
);
sub _build__qyields {
  my $self = shift;
  return $self->_npgqc_data_retriever->retrieve_yields($self->id_run);
}

has '_cluster_density'   =>    ( isa        => 'HashRef',
                                 is         => 'ro',
                                 required   => 0,
                                 lazy_build => 1,
);
sub _build__cluster_density {
  my $self = shift;
  return $self->_npgqc_data_retriever->retrieve_cluster_density($self->id_run);
}

=head2 npg_data

Retrieves data for one run. Returns per-table hashes of key-value pairs that
are suitable for use in updating/creating rows with DBIx.

=cut
sub npg_data {
  my ($self) = @_;

  my $array              = [];
  my $product_array      = [];

  my $dates              = $self->_npg_data_retriever()->dates();
  my $instr              = $self->_npg_data_retriever()->instrument_info;

  my @rlm_column_names =
      $self->_schema_wh->resultset('IseqRunLaneMetric')->result_source->columns();
  my @pm_column_names  =
      $self->_schema_wh->resultset('IseqProductMetric')->result_source->columns();

  foreach my $rs (@{$self->_run_lane_rs})  {

    my $position                    = $rs->position;
    my $values = {};
    $values->{'id_run'}             = $self->id_run;
    $values->{'flowcell_barcode'}   = $rs->run->flowcell_id;
    $values->{'position'}           = $position;
    $values->{'cycles'}             = $rs->run->actual_cycle_count;
    $values->{'cancelled'}          = $self->_run_is_cancelled;
    $values->{'paired_read'}        = $self->_run_is_paired_read;
    $values->{'instrument_name'}    = $instr->{'name'};
    $values->{'instrument_model'}   = $instr->{'model'};

    foreach my $event_type (keys %{$dates}) {
      $values->{$event_type} = $dates->{$event_type};
    }

    foreach my $column (keys %{ $lane_cluster_density->{$position} || {} }) {
      $values->{$column} = $self->_cluster_density->{$position}->{$column};
    }

    if (exists $self->_run_end_summary->{$position}->{$FORWARD_END_INDEX}) {
      $values->{'raw_cluster_count'}  =
        $self->_run_end_summary->{$position}->{$FORWARD_END_INDEX}->{'clusters_raw'};
      $values->{'pf_cluster_count'}   =
        $self->_run_end_summary->{$position}->{$FORWARD_END_INDEX}->{'clusters_pf'};
      $values->{'pf_bases'}           =
        $self->_run_end_summary->{$position}->{$FORWARD_END_INDEX}->{'lane_yield'};
      if (exists $run_end_summary->{$position}->{$REVERSE_END_INDEX}) {
        $values->{'pf_bases'}        +=
        $self->_run_end_summary->{$position}->{$REVERSE_END_INDEX}->{'lane_yield'};
      }
    }

    my $lane_is_indexed = $self->_lane_is_indexed($position);
    my $product_values;
    foreach my $data_hash (($self->_qyields, $self->_autoqc_data)) {
      if ($data_hash->{$position}) {
        foreach my $column (keys %{$data_hash->{$position}}) {
          if ( any {$_ eq $column} @rlm_column_names ) {
	    $values->{$column}          = $data_hash->{$position}->{$column};
	  }
          if ( !$lane_is_indexed && any {$_ eq $column} @pm_column_names ) {
            $product_values->{$column}  = $data_hash->{$position}->{$column};
	  }
	}
      }
    }

    push @{$array}, $values;
    if ($product_values) {
      $product_values->{'id_run'}    = $self->id_run;
      $product_values->{'position'}  = $position;
      push @{$product_array}, $product_values;
    }

    my $plexes = {};
    $plexes = _copy_plex_values($plexes, $self->_autoqc_data, $position);
    $plexes = _copy_plex_values($plexes, $qyields, $position, 1);

    foreach my $tag_index (keys %{$plexes}) {
      my $plex_values             = $plexes->{$tag_index};
      $plex_values->{'id_run'}    = $self->id_run;
      $plex_values->{'position'}  = $position;
      $plex_values->{'tag_index'} = $tag_index;
      push @{$product_array}, $plex_values;
    }
  }

  return {'run_lane' => $array, 'product' => $product_array,};
}

sub _lane_is_indexed {
  my ($self, $position) = @_;
  my $is_indexed = exists $self->_autoqc->{$position}->{'tags_decode_percent'};
  if (!$is_indexed && scalar keys %{$self->_autoqc->{$position}->{$PLEXES_KEY}}) {
    croak qq[Plex autoqc data present for lane $position, but not tag decoding data];
  }
  return $is_indexed;
}

sub _pt_key {
  my @pt = @_;
  if (scalar @pt == 2 && !defined $pt[1]) {
    pop @pt;
  }
  return join q[:], @pt;
}

sub _copy_plex_values {
  my ($destination, $source, $position, $only_existing) = @_;
  if (exists $source->{$position}->{$PLEXES_KEY}) {
    if (scalar keys %{$destination}) {
      foreach my $tag_index (keys %{ $source->{$position}->{$PLEXES_KEY} } ) {
	if ($only_existing && !exists $destination->{$tag_index}) {
	  next;
	}
        foreach my $column_name (keys %{ $source->{$position}->{$PLEXES_KEY}->{$tag_index} } ) {
          $destination->{$tag_index}->{$column_name} =
            $source->{$position}->{$PLEXES_KEY}->{$tag_index}->{$column_name};
	}
      }
    } else {
      $destination = $source->{$position}->{$PLEXES_KEY};
    }
  }
  return $destination;
}

sub _add_lims_fk {
  my ($self, $table, $values) = @_;

  my $position = $values->{'position'};
  my $pt_key = _pt_key($position, $values->{'tag_index'});
  my $pk;

  my @types = keys %{ $self->_flowcell_table_fks->{$position} };

  if ($table eq 'IseqRunLaneMetric' || !$values->{'tag_index'} ) {

    @types = grep { $_ =~ /^library|pool|library_control$/ } @types;
    if (scalar @types > 1) {
      croak q[Lane cannot be all at once: ] . join q[, ], @types;
    }
    $pk = $self->_flowcell_table_fks->{$position}->{$types[0]};

    if ($table eq 'IseqProductMetric' && ($types[0] eq q[pool])) {
      my @samples = keys %{$self->_flowcell_table_fks->{$position}->{'library_indexed'}};
      if (scalar @samples == 1) { # One-sample pool,
                                  # which we processed as a library
        $pk = $self->_flowcell_table_fks->{$position}->{'library_indexed'}->{$samples[0]};
      }
    }

  } else {

    my $pk = $self->_flowcell_table_fks->{$position}->{'library_indexed'}->{$pt_key};
    if (!$index) {
      $pk = $self->_flowcell_table_fks->{$position}->{'library_indexed_spiked'}->{$pt_key};
      if (!$pk && $tag_index == 888) {
        my @spikes = keys %{$self->_flowcell_table_fks->{$position}->{'library_indexed_spiked'}};
        if (scalar @spikes == 1) {
          $pk = $self->_flowcell_table_fks->{$position}->{'library_indexed_spiked'}->{$spikes[0]};
	}
      }
    }

  }

  if ($pk) {
    $values->{'id_iseq_flowcell_tmp'} = $pk;
  }

  return;
}

sub _load_table {
  my ($self, $rows, $table) = @_;

  if (!defined $rows || !@{$rows}) {return;}

  my $rs = $self->_schema_wh->resultset($table);

  if ($self->products_from_scratch && $table eq 'IseqProductMetric') {
    $rs->search({'id_run' => $self->id_run,})->delete();
  }

  foreach my $row (@{$rows}) {
    if($self->verbose) {
      my $message = defined $row->{'tag_index'} ? q[tag_index ] . $row->{'tag_index'} : q[];
      $message = sprintf 'Creating record in table %s for run %i position %i %s',
         $table, $self->id_run,  $row->{'position'}, $message;
      warn "$message\n";
    }
    if ($self->_have_flowcell_table_fks) {
      my $row = $rs->find();
      if (!$row || !$row->id_iseq_flowcell_tmp) { # If the record does not exist
                                                  # or the fk is NULL try to get
                                                  # the value for the fk.
        $self->_add_lims_fk($table, $row);
      }
    }
    $rs->update_or_create($row);
  }

  return;
}

=head2 load

Loads data for one flowcell to the warehouse

=cut
sub load {
  my ($self) = @_;

  my $data;
  try {
    $data = $self->_data();
  } catch {
    warn "$_\n";
  };

  if ($data) {

    my $transaction = sub {
      $self->_load_table($data->{'run_lane'}, 'IseqRunLaneMetric');
      $self->_load_table($data->{'product'},  'IseqProductMetric');
    };

    try {
      $self->_schema_wh->txn_do($transaction);
    } catch {
      my $err = $_;
      if ($err =~ /Rollback failed/sxm) {
        croak $err;
      }
      warn "Failed to load $id_run to $table: $err\n";
    };
  }

  return;
}

__PACKAGE__->meta->make_immutable;

1;

__END__


=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Carp

=item Readonly

=item List::MoreUtils

=item Try::Tiny

=item Moose

=item MooseX::StrictConstructor

=item WTSI:DNAP::Warehouse::Schema

=item npg_tracking::Schema

=item npg_tracking::glossary::run

=item npg_qc::Schema

=item npg_qc::autoqc::qc_store

=item npg_warehouse::loader::autoqc

=item npg_warehouse::loader::qc

=item npg_warehouse::loader::npg

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Marina Gourtovaia

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2014 GRL Genome Research Limited

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
