package npg_warehouse::loader::mqc;

use Carp;
use Moose;
use MooseX::StrictConstructor;
use Readonly;

use npg_qc::Schema;

our $VERSION = '0';

Readonly::Scalar my $COL_NAME_MQC     => 'mqc';
Readonly::Scalar my $COL_NAME_MQC_SEQ => $COL_NAME_MQC . '_seq';
Readonly::Scalar my $COL_NAME_MQC_LIB => $COL_NAME_MQC . '_lib';

=head1 NAME

npg_warehouse::loader::mqc

=head1 SYNOPSIS

=head1 DESCRIPTION

A retriever for manual QC outcomes to be loaded to the warehouse table.
Data for 'mqc_seq', 'mqc_lib' and 'mqc' columns are retrieved as sequencing
qc outcome, library qc outcome and a cumulative value to be used for the product.
Either of the outcomes may be undefined.

=head1 SUBROUTINES/METHODS

=head2 verbose

Verbose flag

=cut

has 'verbose'      => ( isa        => 'Bool',
                        is         => 'ro',
                        required   => 0,
                        default    => 0,
                      );

=head2 schema_qc

DBIx schema object for the NPG QC database

=cut

has 'schema_qc' =>   ( isa        => 'npg_qc::Schema',
                       is         => 'ro',
                       required   => 0,
                       lazy_build => 1,
                     );
sub _build_schema_qc {
  return npg_qc::Schema->connect();
}

=head2 plex_key

Name of the key to use in data structures for plex data.

=cut

has 'plex_key' =>   ( isa             => 'Str',
                      is              => 'ro',
                      required        => 1,
		    );

=head2 retrieve_lane_outcomes

Retrieves manual qc outcomes for a lane as a hash in the format expected
by the warehouse loader.

  my $id_run = 22;
  my $position = 2;

  # Retrieves data for a lane only, tags are not considered
  my $hash_output = $obj->retrieve_lane_outcomes($id_run, $position);
  # or
  my $tags = []; #an array of tag indices
  $hash_output = $obj->retrieve_lane_outcomes($id_run, $position, $tags);

  # Retrieves data for the given tags
  $tags = [2, 3, 5, 7];
  $hash_output = $obj->retrieve_lane_outcomes($id_run, $position, $tags);
  
=cut

sub retrieve_lane_outcomes {
  my ($self, $id_run, $position, $tags) = @_;

  if (!$id_run) {
    croak 'Run id is missing';
  }

  if (!$position) {
    croak 'Position is missing';
  }

  $tags //= [];
  my $outcomes = {};
  $self->_save_outcomes($outcomes,
                        [$COL_NAME_MQC, $COL_NAME_MQC_SEQ, $COL_NAME_MQC_LIB],
                        $tags);

  my $where       = {'id_run' => $id_run, 'position' => $position};
  my $seq_outcome = $self->_seq_outcome($outcomes, $where, $tags);

  if ($seq_outcome) {
    $self->_lib_outcomes($outcomes, $where, $tags);
  }

  return {$position => $outcomes};
}

sub _seq_outcome {
  my ($self, $outcomes, $where, $tags) = @_;

  if (!($outcomes && $where)) {
    croak 'Missing input';
  }

  my $row = $self->schema_qc->resultset('MqcOutcomeEnt')->find($where);
  my $seq_outcome;
  if ($row && $row->has_final_outcome) {
    $seq_outcome = $row->is_accepted ? 1 : 0;
    $self->_save_outcomes($outcomes, [$COL_NAME_MQC, $COL_NAME_MQC_SEQ], $tags, $seq_outcome);
  }

  return $seq_outcome;
}

sub _lib_outcomes {
  my ($self, $outcomes, $where, $tags) = @_;

  if (!($outcomes && $where)) {
    croak 'Missing input';
  }

  $where->{'tag_index'} = ($tags && @{$tags}) ? $tags : undef;
  my $lib_rs = $self->schema_qc->resultset('MqcLibraryOutcomeEnt')->search_autoqc($where);

  while (my $lib_row = $lib_rs->next) {
    if ($lib_row->has_final_outcome) {
      my $lib_outcome = $lib_row->is_accepted ? 1 : ($lib_row->is_rejected ? 0 : undef);
      if (defined $lib_outcome) {
        my $tag_array = defined $lib_row->tag_index ? [$lib_row->tag_index] : [];
        $self->_save_outcomes($outcomes, [$COL_NAME_MQC_LIB], $tag_array, $lib_outcome);
        if (!$lib_outcome) {
          $self->_save_outcomes($outcomes, [$COL_NAME_MQC], $tag_array, $lib_outcome);
	}
      }
    }
  }

  return;
}

sub _save_outcomes {
  my ($self, $outcomes, $outcome_types, $tags, $value) = @_;

  if ($outcomes) {
    $outcome_types //= [];
    $tags          //= [];
    for my $outcome_type ( @{$outcome_types} ) {
      if ( @{$tags} ) {
        my $pk = $self->plex_key;
        for my $t ( @{$tags} ) {
          $outcomes->{$pk}->{$t}->{$outcome_type} = $value;
        }
      } else {
        $outcomes->{$outcome_type} = $value;
      }
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

=item Moose

=item MooseX::StrictConstructor

=item npg_qc::Schema

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Marina Gourtovaia

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2015 Genome Research Limited

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
