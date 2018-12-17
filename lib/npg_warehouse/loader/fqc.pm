package npg_warehouse::loader::fqc;

use Moose;
use MooseX::StrictConstructor;
use Readonly;
use Carp;

use npg_qc::Schema;

our $VERSION = '0';

Readonly::Scalar my $COL_NAME_QC     => 'qc';
Readonly::Scalar my $COL_NAME_QC_SEQ => $COL_NAME_QC . '_seq';
Readonly::Scalar my $COL_NAME_QC_LIB => $COL_NAME_QC . '_lib';

=head1 NAME

npg_warehouse::loader::fqc

=head1 SYNOPSIS

  my $fqc = npg_warehouse::loader::fqc->new(digests => {
    'digest1' => $c1, 'digest2' => $c2, 'digest3' => $c3
  });
  $fqc->retrieve();
  my $outcomes = $fqc->retrive_outcomes('digest1');
  my $outcome  = $fqc->retrive_seq_outcome('22:1');

=head1 DESCRIPTION

A retriever for overall, sequencing and library QC outcomes to be
loaded to the warehouse table. Only final outcomes are retrieved.

For maximum efficiency create an object giving a hash reference of
composition digests (keys) and composition objects (values)for all
entities you will later require QC outcomes for. Outcomes for all
entities are retrieved in one go and are cached by the object for
subsequent retrieval requests.

Giving an empty hash to the constructor does not cause an error.

Requesting an outcome for a digest that was not listed in the
digests hash given to the constructor does not cause an error.
All QC outcomes will be returned as undefined even if the final
outcomes are available in the database.

=head1 SUBROUTINES/METHODS

=head2 digests

A hash reference of composition digests mapped to composition
objects, required attribute.

=cut

has 'digests' => ( isa        => 'HashRef',
                   is         => 'ro',
                   required   => 1,
);

=head2 schema_qc

DBIx schema object for the NPG QC database. An optional
attribute, will be built if not supplied.

=cut

has 'schema_qc' => ( isa        => 'npg_qc::Schema',
                     is         => 'ro',
                     required   => 0,
                     lazy_build => 1,
);
sub _build_schema_qc {
  return npg_qc::Schema->connect();
}

has '_lane_seq_outcomes' => (
                     isa        => 'HashRef',
                     is         => 'ro',
                     required   => 0,
                     lazy_build => 1,

);
sub _build__lane_seq_outcomes {
  my $self = shift;
  my $h = {};
  while (my ($digest, $data) = each %{$self->_outcomes}) {
    if ($data->{'is_single_lane'}) {
      $h->{$data->{'component_lanes'}->[0]} = $data->{'mqc_outcome_ent'};
    }
  }
  return $h;
}

has '_outcomes' => ( isa        => 'HashRef',
                     is         => 'ro',
                     required   => 0,
                     lazy_build => 1,
);
sub _build__outcomes {
  my $self = shift;

  my $outcomes = {};

  my $digests = [keys %{$self->digests}];
  my $rs = $self->schema_qc->resultset('SeqComposition')
                ->search({digest => $digests});

  my $lanes_from_components = sub {
    return map { join q[:], $_->id_run, $_->position } @_;
  };
  my $is_single_lane = sub {
    my @components = @_;
    return (@components == 1 && !defined $components[0]->tag_index) ? 1 : 0;
  };

  while (my $crow = $rs->next) {
    my $digest = $crow->digest;
    for my $related_outcome (qw/
                                mqc_outcome_ent
                                mqc_library_outcome_ent
                               /) {
      my $orow = $crow->$related_outcome;
      # It's a pass or a fail, or, for library QC, a final
      # undefined.
      if ($orow && $orow->has_final_outcome) {
        $outcomes->{$digest}->{$related_outcome} =
          $orow->is_accepted ? 1 :
         ($orow->is_rejected ? 0 : undef);
      }
    }
    my @components = map {$_->seq_component}
                     $crow->seq_component_compositions->all();
    $outcomes->{$crow->digest}->{'is_single_lane'}  = $is_single_lane->(@components);
    $outcomes->{$crow->digest}->{'component_lanes'} = [$lanes_from_components->(@components)];
  }

  foreach my $digest (@{$digests}) {
    if (!exists $outcomes->{$digest}) {
      my @components  = $self->digests->{$digest}->components_list();
      $outcomes->{$digest}->{'is_single_lane'}  = $is_single_lane->(@components);
      $outcomes->{$digest}->{'component_lanes'} = [$lanes_from_components->(@components)];
    }
  }

  return $outcomes;
}

=head2 retrieve

Retrieves and caches qc outcomes. This method should be called prior to
retrieving outcomes for individual entities.

  $obj->retrieve() 

=cut

sub retrieve {
  my $self = shift;
  $self->_outcomes();
  return;
}

=head2 retrieve_outcomes

Retrieves qc outcomes for an entity defined by the argument composition
digest. The return is a hash reference in the format expected by the
warehouse loader (keys as qc, qc_lib and qc_seq and values as 0, 1 or undefined).

  my $qc_outcomes = $obj->retrieve_outcomes('digest1');

Non-final QC outcomes are equivalent to the outcome not being defined.
A fail on sequencing QC leads to the overall fail. A pass on sequencing QC
is overwritten by the library QC outcome, meaning that the overall QC
value would be undefined if the library QC value is undefined. Sequencing QC
outcome for multi-component entities is composed from sequencing QC outcomes
for individual lanes. If all of them are a pass, the value is a pass, if one
of them i a fail, the value is a fail and if none are failed, but one of them
is undefined, the value is undefined.
  
=cut

sub retrieve_outcomes {
  my ($self, $digest) = @_;

  if (!$digest) {
    croak 'Composition digest is required';
  }
  ref $digest && croak 'Digest should be a scalar';

  my $h = {};
  for my $name (($COL_NAME_QC, $COL_NAME_QC_SEQ, $COL_NAME_QC_LIB)) {
    $h->{$name} = undef;
  }

  if (exists $self->_outcomes->{$digest}) {

    my $outcome = $self->_outcomes->{$digest};
    $h->{$COL_NAME_QC_LIB} =   $outcome->{'mqc_library_outcome_ent'};
    $h->{$COL_NAME_QC_SEQ} =   $outcome->{'mqc_outcome_ent'};
    my $num_components     = @{$outcome->{'component_lanes'}};
    my $is_single_lane     =   $outcome->{'is_single_lane'};

    if (!defined $h->{$COL_NAME_QC_SEQ} && !$is_single_lane) {
      $h->{$COL_NAME_QC_SEQ} = $self->_get_lane_seq_outcome($digest);
    }

    $h->{$COL_NAME_QC} = $h->{$COL_NAME_QC_SEQ};
    if ($h->{$COL_NAME_QC}) {
      # No overwrite for a single lane.
      if ( defined $h->{$COL_NAME_QC_LIB} || (($num_components > 1) || !$is_single_lane) ) {
        $h->{$COL_NAME_QC} = $h->{$COL_NAME_QC_LIB};
      }
    }
  }

  return $h;
}

=head2 retrieve_seq_outcome

Similar to retrieve_outcomes, but retrieves only sequencing
outcome for an entity represented by the argument lane rpt key.

If the outcome for this lane is not already cached, an attempt
to retrieve and cache it will be made.

  my $outcome = $obj->retrieve_seq_outcome('22:1');

=cut

sub retrieve_seq_outcome {
  my ($self, $rpt_key) = @_;

  if (!$rpt_key) {
    croak 'rpt key is required';
  }
  ref $rpt_key && croak 'Digest should be a scalar';

  if (!exists $self->_lane_seq_outcomes->{$rpt_key}) {
    $self->_cache_lane_seq_outcome($rpt_key);
  }

  return {$COL_NAME_QC_SEQ => $self->_lane_seq_outcomes->{$rpt_key}};
}

sub _cache_lane_seq_outcome {
  my ($self, $lane_key) = @_;

  my $cached = 0;
  my @attr = split /:/smx, $lane_key;
  my $orow = $self->schema_qc->resultset('MqcOutcomeEnt')
    ->search({id_run => $attr[0], position => $attr[1]})->next;
  if ($orow && $orow->has_final_outcome) {
    $self->_lane_seq_outcomes->{$lane_key} = $orow->is_accepted ? 1 : 0;
    $cached = 1;
  }

  return $cached;
}

sub _get_lane_seq_outcome {
  my ($self, $digest) = @_;

  my $undef_outcome_present = 0;

  foreach my $lane_key (@{$self->_outcomes->{$digest}->{'component_lanes'}}) {
    if (!exists $self->_lane_seq_outcomes->{$lane_key}) {
      $self->_cache_lane_seq_outcome($lane_key);
    }
    my $o = $self->_lane_seq_outcomes->{$lane_key};
    if (defined $o) {
      if ($o == 0) {
        return 0;
      }
     } else {
       $undef_outcome_present = 1;
     }
  }

  return $undef_outcome_present ? undef : 1;
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

Copyright (C) 2018 Genome Research Limited

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
