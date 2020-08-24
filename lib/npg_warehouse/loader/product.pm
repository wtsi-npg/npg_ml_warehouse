package npg_warehouse::loader::product;

use Moose::Role;
use Readonly;
use Carp;
use Clone qw/clone/;

use npg_warehouse::loader::fqc;
use npg_warehouse::loader::autoqc;

requires qw/schema_qc schema_wh/;

our $VERSION  = '0';

Readonly::Scalar my $PRODUCT_TABLE_NAME  => q[IseqProductMetric];
Readonly::Scalar my $LIMS_FK_COLUMN_NAME => q[id_iseq_flowcell_tmp];

=head1 NAME

npg_warehouse::loader::product

=head1 SYNOPSIS

=head1 DESCRIPTION

A Moose role providing methods for retrieving product data and
loading product data to the ml warehouse.

If a consuming class implements get_lims_fk method, the rows,
if appropriate, will be linked to the iseq_flowcell table.

=head1 SUBROUTINES/METHODS

=head2 product_data

=cut

sub product_data {
  my ($self, $data_hash, $lane_data) = @_;

  my $indexed_lanes = _indexed_lanes_hash($data_hash);
  my %digests = map { $_ => $data_hash->{$_}->{'composition'} }
                keys %{$data_hash};
  my $fqc_retriever = npg_warehouse::loader::fqc->new(
    schema_qc => $self->schema_qc,
    digests   => \%digests
  );
  $fqc_retriever->retrieve();

  if ($lane_data) {
    foreach my $position (keys %{$lane_data}) {
      $self->_copy_lane_data($lane_data, $position,
        $fqc_retriever->retrieve_seq_outcome(
          join q[:], $lane_data->{$position}->{'id_run'}, $position));
    }
  }

  my @products = ();
  while (my ($product_digest, $data) = each %{$data_hash}) {

    my $composition = $data->{'composition'};
    if ($composition->num_components == 1) {
      my $component = $composition->get_component(0);
      my $position  = $component->position;
      my $tag_index = $component->tag_index;
      if (!defined $tag_index) { # Lane data
        if ($lane_data) {
          $self->_copy_lane_data($lane_data, $position, $data);
        }
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
      my %id_runs_h = ();
      my %tis_h     = ();
      foreach my $c ($composition->components_list()) {
        $id_runs_h{$c->id_run} = 1;
        $tis_h{defined $c->tag_index ? $c->tag_index : 'no_index'} = 1;
      }
      if (scalar keys %id_runs_h == 1) {
        $data->{'id_run'} = (keys %id_runs_h)[0];
        if (scalar keys %tis_h == 1) {
          $data->{'tag_index'} = (keys %tis_h)[0];
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

  return [sort {_compare_product_data($a, $b)} @products];
}

=head2 load_iseqproductmetric_table

 If the row is being updated, we are not going to touch the foreign
 key into iseq_flowcell table, unless we have a method to do this
 (get_lims_fk) and the value of the lims_fk_repair attribute is set
 to true.

 If get_lims_fk exists and the row is being created, we will try to
 assign the foreign key value regardless of the value of the
 lims_fk_repair attribute.

 Multi-component entities do not have a corresponding entry in
 iseq_flowcell table, no attempt is made to set the value of the
 foreign key for them.

 If the parent row in the iseq_flowcell table has been deleted,
 the foreign key value has been set to NULL. Resetting it to a valid
 value is the responsibility of the daemon that repairs foreign keys,
 which will set the lims_fk_repair flag to true for this object.

=cut

sub load_iseqproductmetric_table {
  my ($self, $data_list) = @_;

  my @rows = ();

  foreach my $original_row (@{$data_list}) {
    my $row = clone($original_row);
    my $composition    = delete $row->{'composition'};
    my $num_components = delete $row->{'num_components'};
    $self->_filter_column_names($row);
    push @rows, {data           => $row,
                 num_components => $num_components,
                 composition    => $composition};
  }

  my $rs = $self->schema_wh->resultset($PRODUCT_TABLE_NAME);
  my $transaction = sub {
    foreach my $h (@rows) {
      my $action = 'updated';
      my $result = $rs->find_or_new($h->{'data'});
      # Multi-component entities do not have a corresponding entry in LIMs
      my $calc_fk = ($h->{'num_components'} == 1) && $self->can('get_lims_fk');
      if (!$result->in_storage) {
        $action = 'created';
        if ($calc_fk) {
          my $fk = $self->get_lims_fk($result);
          $fk && $result->set_column($LIMS_FK_COLUMN_NAME => $fk);
	}
        $result = $result->insert();
        $self->_create_linking_rows(
          $result, $h->{'num_components'}, $h->{'composition'});
      } else {
        if ($calc_fk && $self->lims_fk_repair) {
          # There might be a legitimate reason to set the value of the
          # foreign key to NULL, so we do this unconditionally.
          $h->{'data'}->{$LIMS_FK_COLUMN_NAME} = $self->get_lims_fk($result);
        }
        $result->update($h->{'data'});
      }
      my $m = "$PRODUCT_TABLE_NAME row $action for " .
              $h->{'data'}->{'iseq_composition_tmp'};
      $self->can('info') ? $self->info($m) : warn "$m\n";
    }
  };

  $self->schema_wh->txn_do($transaction);

  return scalar @rows;
}

sub _copy_lane_data {
  my ($self, $lane_data, $p, $h) = @_;

  while (my ($column_name, $value) = each %{$h}) {
    $lane_data->{$p}->{$column_name} = $value;
  }
  $lane_data->{$p}->{'position'} = $p;

  return;
}

sub _compare_product_data {
  my ($a, $b) = @_;
  # Data for single component first
  my $r = $a->{'num_components'} <=> $b->{'num_components'};
  return $r if $r != 0;
  return $a->{'iseq_composition_tmp'} cmp $b->{'iseq_composition_tmp'};
}

sub _indexed_lanes_hash {
  my $data_hash = shift;
  my %pools =
      map  { $_->position => 1}
      grep { defined $_->tag_index }
      map  { $_->get_component(0) }
      grep { $_->num_components == 1 }
      map  { $_->{'composition'} }
      values %{$data_hash};
  return \%pools;
}

sub _filter_column_names {
  my ($self, $values) = @_;

  my $pp_prefix = $npg_warehouse::loader::autoqc::PP_PREFIX;
  my @columns = keys %{$values};
  foreach my $name (@columns) {
    if ($name =~ /\A$pp_prefix/smx) {
      delete $values->{$name};
      next;
    }
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

sub _create_linking_rows {
  my ($self, $result, $num_components, $composition) = @_;

  my $rs  = $self->schema_wh->resultset($PRODUCT_TABLE_NAME);
  my $rsl = $self->schema_wh->resultset('IseqProductComponent');
  my $pk_name = 'id_iseq_pr_metrics_tmp';
  my $row_pk = $result->$pk_name;

  my $create_row = sub {
    my ($pcid, $i) = @_;
    $rsl->create({id_iseq_pr_tmp           => $row_pk,
                  id_iseq_pr_component_tmp => $pcid,
                  component_index          => $i,
                  num_components           => $num_components});
  };

  if ($num_components == 1) {
    $create_row->($row_pk, 1);
  } else {
    my $count = 1;
    foreach my $component ($composition->components_list) {
      my $db_component = $rs->search({
        id_run    => $component->id_run,
        position  => $component->position,
        tag_index => $component->tag_index})->next;
      $db_component or croak
        'Failed to find the component product row for ' . $component->freeze();
      $create_row->($db_component->$pk_name, $count);
      $count++;
    }
  }

  return;
}

no Moose::Role;

1;

__END__

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Carp

=item Readonly

=item Moose::Role

=item Clone

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Marina Gourtovaia

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2019,2020 Genome Research Ltd.

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
