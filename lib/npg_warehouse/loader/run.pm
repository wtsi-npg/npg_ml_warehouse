package npg_warehouse::loader::run;

use Moose;
use MooseX::StrictConstructor;
use Try::Tiny;
use List::MoreUtils qw/ any uniq /;
use Readonly;
use Carp;

use npg_tracking::glossary::composition;
use npg_tracking::glossary::composition::component::illumina;
use npg_qc::autoqc::qc_store;

use npg_warehouse::loader::autoqc;
use npg_warehouse::loader::qc;
use npg_warehouse::loader::fqc;
use npg_warehouse::loader::npg;

extends 'npg_warehouse::loader::base';

with qw/
  npg_tracking::glossary::run
  npg_tracking::glossary::flowcell
  npg_warehouse::loader::product
       /;
with 'WTSI::DNAP::Warehouse::Schema::Query::IseqFlowcell';

our $VERSION  = '0';

Readonly::Scalar my $NON_INDEXED_LIBRARY   => $WTSI::DNAP::Warehouse::Schema::Query::IseqFlowcell::NON_INDEXED_LIBRARY;
Readonly::Scalar my $CONTROL_LANE          => $WTSI::DNAP::Warehouse::Schema::Query::IseqFlowcell::CONTROL_LANE;
Readonly::Scalar my $INDEXED_LIBRARY       => $WTSI::DNAP::Warehouse::Schema::Query::IseqFlowcell::INDEXED_LIBRARY;
Readonly::Scalar my $INDEXED_LIBRARY_SPIKE => $WTSI::DNAP::Warehouse::Schema::Query::IseqFlowcell::INDEXED_LIBRARY_SPIKE;

Readonly::Scalar my $FLOWCELL_LIMS_TABLE_NAME => q[IseqFlowcell];
Readonly::Scalar my $RUN_LANE_TABLE_NAME      => q[IseqRunLaneMetric];
Readonly::Scalar my $PRODUCT_TABLE_NAME       => q[IseqProductMetric];
Readonly::Scalar my $LIMS_FK_COLUMN_NAME      => q[id_iseq_flowcell_tmp];

Readonly::Scalar my $SPIKE_FALLBACK_TAG_INDEX => 888;

=head1 NAME

npg_warehouse::loader::run

=head1 SYNOPSIS

 npg::warehouse::loader::run->new(id_run => 4444)->load;

=head1 DESCRIPTION

=head1 SUBROUTINES/METHODS

=head2 id_run

Run id

=head2 id_flowcell_lims

LIMs specific flowcell id

=cut
has '+id_flowcell_lims'  => ( lazy_build => 1,);
sub _build_id_flowcell_lims {
  my $self = shift;
  return $self->_run_lane_rs->[0]->run->batch_id;
}

=head2 id_flowcell_manufacturer

Manufacturer flowcell id

=cut
has '+flowcell_barcode'  => ( lazy_build => 1,);
sub _build_flowcell_barcode {
  my $self = shift;
  return $self->_run_lane_rs->[0]->run->flowcell_id;
}

=head2 iseq_flowcell

DBIx result set from which relevant flowcell rows can be retrieved

=cut
sub iseq_flowcell {
  my $self = shift;
  return $self->schema_wh->resultset($FLOWCELL_LIMS_TABLE_NAME);
}

has '_flowcell_table_fks' => ( isa        => 'HashRef',
                               is         => 'ro',
                               required   => 0,
                               lazy_build => 1,
);
sub _build__flowcell_table_fks {
  my $self = shift;

  my $fks = {};
  my $rs;
  try {
    $rs = $self->query_resultset;
  } catch {
    my $allowed_error = qr/id_flowcell_lims\sor\sflowcell_barcode/xms;
    my $error = $_;
    if ($error !~ $allowed_error) {
      croak $error;
    }
  };

  if (!$rs) {
    if ($self->explain) {
      warn q[Tracking database has no flowcell information for run ] . $self->id_run . qq[\n];
    }
    return $fks;
  }

  my @to_delete = ();
  while (my $row = $rs->next()) {
    my $entity_type = $row->entity_type;
    my $position    = $row->position;
    my $pt_key = _pt_key($position, $row->tag_index);
    if (exists $fks->{$position}->{$entity_type}->{$pt_key}) {
      warn sprintf 'Run %i: multiple flowcell table records for %s, pt key %s%s',
        $self->id_run, $entity_type, $pt_key, qq[\n];
      push @to_delete, [$position, $entity_type, $pt_key];
    }
    $fks->{$position}->{$entity_type}->{$pt_key} = $row->$LIMS_FK_COLUMN_NAME;
  }

  foreach my $d (@to_delete) {
    delete $fks->{$d->[0]}->{$d->[1]}->{$d->[2]};
  }

  if ($self->explain && (scalar keys %{$fks} == 0)) {
    warn q[Flowcell table has no LIMs information for run ] . $self->id_run . qq[\n];
  }

  return $fks;
}

has '_flowcell_table_fks_exist' => ( isa        => 'Bool',
                                     is         => 'ro',
                                     required   => 0,
                                     lazy_build => 1,
);
sub _build__flowcell_table_fks_exist {
  my $self = shift;
  return scalar keys %{$self->_flowcell_table_fks} ? 1 : 0;
}

has '_autoqc_store' =>    ( isa        => 'npg_qc::autoqc::qc_store',
                            is         => 'ro',
                            required   => 0,
                            lazy_build => 1,
);
sub _build__autoqc_store {
  my $self = shift;
  return npg_qc::autoqc::qc_store->new(use_db    => 1,
                                       verbose   => $self->verbose,
                                       qc_schema => $self->schema_qc);
}

has '_run_lane_rs' =>     ( isa        => 'ArrayRef',
                            is         => 'ro',
                            required   => 0,
                            lazy_build => 1,
);
sub _build__run_lane_rs {
  my $self = shift;
  my @all_rs = $self->schema_npg->resultset('RunLane')->search(
    { q[me.id_run] => $self->id_run},
    {
      prefetch => q[run],
      order_by => [q[me.id_run], q[me.position]],
    },
  )->all;
  return \@all_rs;
}

has '_old_forward_id_run'  => ( isa        => 'Int',
                                is         => 'ro',
                                required   => 0,
                                lazy_build => 1,
);
sub _build__old_forward_id_run {
  my $self= shift;
  my $rp = $self->_run_lane_rs->[0]->run->id_run_pair;
  $rp ||= 0;
  return $rp;
}

has '_npg_data_retriever'   =>   ( isa        => 'npg_warehouse::loader::npg',
                                   is         => 'ro',
                                   required   => 0,
                                   lazy_build => 1,
);
sub _build__npg_data_retriever {
  my $self = shift;
  return npg_warehouse::loader::npg->new(schema_npg => $self->schema_npg,
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
  return npg_warehouse::loader::qc->new(schema_qc => $self->schema_qc);
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

has '_data'              =>    ( isa        => 'HashRef',
                                 is         => 'ro',
                                 required   => 0,
                                 lazy_build => 1,
);
sub _build__data {
  my $self = shift;

  my $dates = $self->_npg_data_retriever()->dates();
  my $instr = $self->_npg_data_retriever()->instrument_info;
  my $lane_data = {};

  foreach my $rs (@{$self->_run_lane_rs})  {

    my $position                    = $rs->position;
    my %values = %{$instr};

    $values{'id_run'}             = $self->id_run;
    $values{'flowcell_barcode'}   = $rs->run->flowcell_id;
    $values{'position'}           = $position;
    $values{'cycles'}             = $rs->run->actual_cycle_count;
    $values{'run_priority'}       = $rs->run->priority;
    $values{'cancelled'}          = $self->_run_is_cancelled;
    $values{'paired_read'}        = $self->_run_is_paired_read;

    foreach my $event_type (keys %{$dates}) {
      $values{$event_type} = $dates->{$event_type};
    }

    foreach my $column (keys %{ $self->_cluster_density->{$position} || {} }) {
      $values{$column} = $self->_cluster_density->{$position}->{$column};
    }
    $lane_data->{$position} = \%values;
  }

  my @run_lane_columns = $self->schema_wh
    ->resultset($RUN_LANE_TABLE_NAME)->result_source->columns();
  my @interop_column_names = grep { /\Ainterop_/xms } @run_lane_columns;

  my $data_hash = npg_warehouse::loader::autoqc
    ->new(mlwh => 1,
          autoqc_store => $self->_autoqc_store,
          interop_data_column_names => \@interop_column_names)
    ->retrieve($self->id_run, $self->schema_npg);
  my $product_data = $self->product_data($data_hash, $lane_data);

  my %known_column_names = map { $_ => 1 } @run_lane_columns;

  my @lane_data_list = ();
  foreach my $lane (values %{$lane_data}) {
    foreach my $column_name (keys %{$lane}) {
      exists $known_column_names{$column_name} or delete $lane->{$column_name};
    }
    $lane->{'id_run'} //= $self->id_run;
    $lane->{'cycles'} //= 0;

    push @lane_data_list, $lane;
  }
  @lane_data_list = sort {$a->{position} <=> $b->{position}} @lane_data_list;

  return { $RUN_LANE_TABLE_NAME => \@lane_data_list,
	   $PRODUCT_TABLE_NAME  => $product_data };
}

sub _pt_key {
  my ($p, $t) = @_;
  return defined $t ? join(q[:], $p, $t) : $p;
}

sub _explain_missing {
  my ($self, $pt_key, $position, $ti) = @_;
  if ($self->explain) {
    if (!defined $ti || $ti != 0) {
      my $lib_type = defined $ti ? $NON_INDEXED_LIBRARY : $INDEXED_LIBRARY;
      my @keys = keys %{$self->_flowcell_table_fks->{$position}->{$lib_type}};
      my $other_keys = @keys ? join(q[ ], @keys) : 'none';
      warn sprintf 'Flowcell table has no information for pt key %s, run %i; other keys %s%s',
        $pt_key, $self->id_run, $other_keys, qq[\n];
    }
  }
  return;
}

sub _load_iseq_run_lane_metrics_table {
  my $self = shift;
  my $transaction = sub {
    my $count = 0;
    my $rs = $self->schema_wh->resultset($RUN_LANE_TABLE_NAME);
    foreach my $row (@{$self->_data->{$RUN_LANE_TABLE_NAME}}) {
      if ($self->verbose) {
        warn "Will update or create record in $RUN_LANE_TABLE_NAME for " .
        join q[ ], 'run', $row->{'id_run'}, 'position', $row->{'position'} . "\n";
      }
      $rs->update_or_create($row);
      $count++;
    }
    return $count;
  };

  return $self->schema_wh->txn_do($transaction);
}

=head2 get_lims_fk

This method tries to compute a value for the foreign key into
iseq_flowcell table for the argument iseq_product_metrics table
row. If does not try to evaluate whether it's appropriate for
the argument row to have this value set.

Undefined value is returned if no matching single(!) row is found
in iseq_flowcell table

=cut

sub get_lims_fk {
  my ($self, $row) = @_;

  return if !$self->_flowcell_table_fks_exist;

  my $position = $row->position;
  my $ti       = $row->tag_index;

  my @types = exists $self->_flowcell_table_fks->{$position} ?
              keys %{ $self->_flowcell_table_fks->{$position} } : ();
  if (!@types) {
    if ($self->verbose) {
       warn "Flowcell table has no information for lane $position run " . $self->id_run . "\n";
    }
    return;
  }

  my $pt_key = _pt_key($position, $ti);
  my $pk;

  if (!defined $ti) {

    my @lane_types = grep { /^(?: $NON_INDEXED_LIBRARY | $CONTROL_LANE )$/xms } @types;
    if (scalar @lane_types > 1) {
      croak q[Lane cannot be both ] . join q[ and  ], @types;
    }

    if (!@lane_types) {
      my @plexes = keys %{$self->_flowcell_table_fks->{$position}->{$INDEXED_LIBRARY}};
      if (scalar @plexes == 1) {  # one-sample pool,
                                  # which we processed as a library
        $pk = $self->_flowcell_table_fks->{$position}->{$INDEXED_LIBRARY}->{$plexes[0]};
      }
    } else {
      $pk = $self->_flowcell_table_fks->{$position}->{$lane_types[0]}->{$pt_key};
      if (!$pk && ($lane_types[0] eq $NON_INDEXED_LIBRARY)) {
        # Check for a crazy case of a library with tag index 1 -
        # some sort of artifact of loading iseq_flowcell table.
        $pt_key = _pt_key($position, 1);
        $pk = $self->_flowcell_table_fks->{$position}->{$lane_types[0]}->{$pt_key};
      }
    }

  } else {

    $pk = $self->_flowcell_table_fks->{$position}->{$INDEXED_LIBRARY}->{$pt_key};
    if (!$pk) {
      $pk = $self->_flowcell_table_fks->{$position}->{$INDEXED_LIBRARY_SPIKE}->{$pt_key};
      if (!$pk && ($ti == $SPIKE_FALLBACK_TAG_INDEX)) {
        my @spikes = keys %{$self->_flowcell_table_fks->{$position}->{$INDEXED_LIBRARY_SPIKE}};
        if (scalar @spikes == 1) {
          $pk = $self->_flowcell_table_fks->{$position}->{$INDEXED_LIBRARY_SPIKE}->{$spikes[0]};
        }
      }
    }

  }

  if (!$pk) {
    $self->_explain_missing($pt_key, $position, $ti);
  }

  return $pk;
}

=head2 load

Loads data for one sequencing run to the warehouse

=cut
sub load {
  my ($self) = @_;

  my $id_run = $self->id_run;

  if (! @{$self->_run_lane_rs}) {
    if($self->verbose) {
      warn qq[No lanes for run $id_run, not loading\n];
    }
    return;
  }

  if ($self->_old_forward_id_run) {
    if ($self->verbose) {
      warn sprintf 'Run %i is an old reverse run for %i, not loading.%s',
        $id_run, $self->_old_forward_id_run, qq[\n];
    }
    return;
  }

  if (!$self->_npg_data_retriever->run_ready2load) {
    if($self->verbose) {
      warn qq[Too early to load run $id_run, not loading\n];
    }
    return;
  }

  my $data;
  try {
    $data = $self->_data();
  } catch {
    warn "$_\n";
  };

  return if !$data;

  foreach my $table (($RUN_LANE_TABLE_NAME, $PRODUCT_TABLE_NAME)) {
    if (!defined $self->_data->{$table} || scalar @{$self->_data->{$table}} == 0) {
      if ($self->verbose) {
        warn qq[No data for table $table\n];
      }
    } else {
      my $count;
      try {
        $count = $table eq $RUN_LANE_TABLE_NAME ?
                 $self->_load_iseq_run_lane_metrics_table() :
                 $self->load_iseq_product_metrics_table($self->_data->{$table});
        if ($self->verbose) {
          warn qq[Loaded $count rows to table $table for run $id_run\n];
        }
      } catch {
        my $err = $_;
        if ($err =~ /Rollback failed/sxm) {
          croak $err;
        }
        warn qq[Failed to load run $id_run: $err\n];
      };
      defined $count or last;
    }
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

=item WTSI::DNAP::Warehouse::Schema::Query::IseqFlowcell

=item npg_tracking::glossary::run

=item npg_tracking::glossary::flowcell

=item npg_qc::autoqc::qc_store

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Marina Gourtovaia

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2018,2019,2020 Genome Research Ltd.

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
