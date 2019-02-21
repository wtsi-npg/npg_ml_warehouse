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
       /;
with 'WTSI::DNAP::Warehouse::Schema::Query::IseqFlowcell';

our $VERSION  = '0';

Readonly::Scalar my $NON_INDEXED_LIBRARY      => $WTSI::DNAP::Warehouse::Schema::Query::IseqFlowcell::NON_INDEXED_LIBRARY;
Readonly::Scalar my $CONTROL_LANE             => $WTSI::DNAP::Warehouse::Schema::Query::IseqFlowcell::CONTROL_LANE;
Readonly::Scalar my $INDEXED_LIBRARY          => $WTSI::DNAP::Warehouse::Schema::Query::IseqFlowcell::INDEXED_LIBRARY;
Readonly::Scalar my $INDEXED_LIBRARY_SPIKE    => $WTSI::DNAP::Warehouse::Schema::Query::IseqFlowcell::INDEXED_LIBRARY_SPIKE;

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

=head2 lims_fk_repair

Boolean flag, false by default. Switches on and off
repair of LIMs foreign key values for update operations.

=cut
has 'lims_fk_repair' => ( isa      => 'Bool',
                          is       => 'ro',
                          required => 0,
);

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

has '_rl_column_names' => (
  isa        => 'ArrayRef',
  is         => 'ro',
  required   => 0,
  lazy_build => 1,
);
sub _build__rl_column_names {
  my $self = shift;
  return [$self->schema_wh->resultset($RUN_LANE_TABLE_NAME)->result_source->columns()];
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
  my $lane_deplexed_flags = {};

  my $data_hash = npg_warehouse::loader::autoqc
    ->new(autoqc_store => $self->_autoqc_store, mlwh => 1)
    ->retrieve($self->id_run, $self->schema_npg);
  my $indexed_lanes = _indexed_lanes_hash($data_hash);

  my %digests = map { $_ => $data_hash->{$_}->{'composition'} }
                keys %{$data_hash};
  my $fqc_retriever = npg_warehouse::loader::fqc->new(
    schema_qc => $self->schema_qc,
    digests   => \%digests
  );
  $fqc_retriever->retrieve();

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

    $self->_copy_lane_data($lane_data, $position,
      $fqc_retriever->retrieve_seq_outcome(join q[:], $self->id_run, $position));
  }

  my @products = ();

  while (my ($product_digest, $data) = each %{$data_hash}) {

    my $composition = $data->{'composition'};
    if ($composition->num_components == 1) {
      my $component = $composition->get_component(0);
      my $position  = $component->position;
      my $tag_index = $component->tag_index;
      if (!defined $tag_index) { # Lane data
        $self->_copy_lane_data($lane_data, $position, $data);
        # If this is lane data for an indexed lane, the lane itself is
        # not the end product.
        if ($data->{'tags_decode_percent'} || $indexed_lanes->{$position}) {
          next;
	}
      }
      $data->{'id_run'}    = $component->id_run;
      $data->{'position'}  = $position;
      $data->{'tag_index'} = $tag_index;
    } else {
      my @id_runs = uniq map { $_->id_run } $composition->components_list();
      if (scalar @id_runs == 1) {
        $data->{'id_run'} = $id_runs[0];
      }
      my @tis = grep { defined }
                map  { $_->tag_index }
                $composition->components_list();
      if (scalar @tis == $composition->num_components) {
        @tis = uniq @tis;
        if (scalar @tis == 1) {
          $data->{'tag_index'} = $tis[0];
	}
      }
    }

    my $qc_outcomes = $fqc_retriever->retrieve_outcomes($product_digest);
    while ( my ($column_name, $value) = each %{$qc_outcomes} ) {
      $data->{$column_name} = $value;
    }

    $data->{'id_iseq_product'}      = $composition->digest();
    $data->{'iseq_composition_tmp'} = $composition->freeze();
    $data->{'num_components'}       = $composition->num_components();
    push @products, $data;
  }

  return { $RUN_LANE_TABLE_NAME =>
           [sort {$a->{position} <=> $b->{position}} values %{$lane_data}],
           $PRODUCT_TABLE_NAME  =>
           [sort {_compare_product_data($a, $b)} @products] };
}

sub _compare_product_data {
  my ($a, $b) = @_;
  # Data for single component first
  my $r = $a->{'num_components'} <=> $b->{'num_components'};
  return $r if $r != 0;
  return $a->{'iseq_composition_tmp'} cmp $b->{'iseq_composition_tmp'};
}

sub _copy_lane_data {
  my ($self, $lane_data, $p, $h) = @_;

  while (my ($column_name, $value) = each %{$h}) {
    if (any { $_ eq $column_name } @{$self->_rl_column_names()}) {
      $lane_data->{$p}->{$column_name} = $value;
    }
  }

  $lane_data->{$p}->{'position'} = $p;
  $lane_data->{$p}->{'id_run'} = $self->id_run;

  if (!defined $lane_data->{$p}->{'cycles'}) {
    $lane_data->{$p}->{'cycles'} = 0;
  }

  return;
}

sub _indexed_lanes_hash {
  my $data_hash = shift;
  my %pools =
      map  {$_ => 1 }
      uniq
      map  { $_->position }
      grep { defined $_->tag_index }
      map  { $_->get_component(0) }
      grep { $_->num_components == 1 }
      map  { $_->{'composition'} }
      values %{$data_hash};
  return \%pools;
}

sub _pt_key {
  my ($p, $t) = @_;
  return defined $t ? join(q[:], $p, $t) : $p;
}

sub _get_lims_fk {
  my ($self, $row) = @_;

  return if !$self->_flowcell_table_fks_exist;

  my $position;
  my $ti;

  # The argument $row can be either a hash reference or a DBIx::Class::Row object.
  if (ref $row eq 'HASH') {
    $position = $row->{'position'};
    $ti       = $row->{'tag_index'};
  } else {
    $position = $row->position;
    $ti       = $row->tag_index;
  }

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

sub _filter_column_names {
  my ($self, $values) = @_;

  my @columns = keys %{$values};
  foreach my $name (@columns) {
    my $old_name = $name;
    my $count = $name =~ s/\Atag_sequence\Z/tag_sequence4deplexing/xms;
    if (!$count) {
      $count = $name =~ s/\Abam_//xms;
    }
    if ($count) {
      $values->{$name} = $values->{$old_name};
      delete $values->{$old_name};
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

sub _load_iseq_product_metrics_table {
  my $self = shift;

  #####
  # If the row is being updated, we are not going to touch the foreign
  # key into iseq_flowcell table, unless lims_fk_repair is set to true.
  # If the row is being created, we will try to assign this foreing key.
  # value. If the parent row in the iseq_flowcell table has been deleted,
  # the foreign key value has been set to NULL. Resetting it to a valid
  # value is the responsibility of the daemon that repairs foreign keys,
  # which will set the lims_fk_repair flag to true for this object.
  #

  my @rows = ();
  foreach my $row (@{$self->_data->{$PRODUCT_TABLE_NAME}}) {
    my $composition    = delete $row->{'composition'};
    my $num_components = delete $row->{'num_components'};
    $self->_filter_column_names($row);
    if ($self->lims_fk_repair && ($num_components == 1)) {
      $row->{$LIMS_FK_COLUMN_NAME} = $self->_get_lims_fk($row);
    }
    push @rows, {data           => $row,
                 num_components => $num_components,
                 composition    => $composition};
  }

  my $rs = $self->schema_wh->resultset($PRODUCT_TABLE_NAME);
  my $transaction = sub {
    foreach my $h (@rows) {
      my $action = 'updated';
      my $result = $rs->update_or_new($h->{'data'});
      if (!$result->in_storage) {
        $action = 'created';
        # Try to get the fk value if this has not been already done.
        if (!$self->lims_fk_repair && ($h->{'num_components'} == 1)) {
          my $fk = $self->_get_lims_fk($result);
          if ($fk) {
            $result->set_column($LIMS_FK_COLUMN_NAME => $fk);
	  }
        }
        $result = $result->insert();
        $self->_create_linking_rows(
          $result, $h->{'num_components'}, $h->{'composition'});
      }
      if ($self->verbose) {
        warn "$PRODUCT_TABLE_NAME row $action for " .
           $h->{'data'}->{'iseq_composition_tmp'} . "\n";
      }
    }
  };

  $self->schema_wh->txn_do($transaction);

  return scalar @rows;
}

sub _create_linking_rows {
  my ($self, $result, $num_components, $composition) = @_;

  my $rs  = $self->schema_wh->resultset($PRODUCT_TABLE_NAME);
  my $rsl = $self->schema_wh->resultset('IseqProductComponent');
  my $pk_name = 'id_iseq_pr_metrics_tmp';
  my $row_pk = $result->$pk_name;

  my $create_row = sub {
    my $pcid = shift;
    $rsl->create({id_iseq_pr_tmp           => $row_pk,
                  id_iseq_pr_component_tmp => $pcid,
                  num_components           => $num_components});
  };

  if ($num_components == 1) {
    $create_row->($row_pk);
  } else {
    foreach my $component ($composition->components_list) {
      my $db_component = $rs->search({
        id_run    => $component->id_run,
        position  => $component->position,
        tag_index => $component->tag_index})->next;
      $db_component or croak
        'Failed to find a row for ' . $component->freeze();
      $create_row->($db_component->$pk_name);
    }
  }

  return;
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
                 $self->_load_iseq_product_metrics_table();
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

Copyright (C) 2018, 2019 Genome Research Limited

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
