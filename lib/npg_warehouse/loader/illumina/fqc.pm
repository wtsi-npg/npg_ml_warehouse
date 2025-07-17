package npg_warehouse::loader::illumina::fqc;

use Moose;
use MooseX::StrictConstructor;
use Readonly;
use Carp;

use npg_qc::Schema;
use npg_tracking::glossary::composition;

our $VERSION = '0';

Readonly::Scalar my $COL_NAME_QC      => 'qc';
Readonly::Scalar my $COL_NAME_QC_SEQ  => $COL_NAME_QC . '_seq';
Readonly::Scalar my $COL_NAME_QC_LIB  => $COL_NAME_QC . '_lib';
Readonly::Scalar my $COL_NAME_QC_USER => $COL_NAME_QC . '_user';
Readonly::Scalar my $SEQ_OUTCOME_ENT  => 'mqc_outcome_ent';
Readonly::Scalar my $LIB_OUTCOME_ENT  => 'mqc_library_outcome_ent';
Readonly::Scalar my $UQC_OUTCOME_ENT  => 'uqc_outcome_ent';
Readonly::Scalar my $COMPONENT_LANES  => 'component_lanes';
Readonly::Scalar my $IS_SINGLE_LANE   => 'is_single_lane';
Readonly::Scalar my $MAX_NUMBER_DIGESTS => 900;

=head1 NAME

npg_warehouse::loader::illumina::fqc

=head1 SYNOPSIS

  my $fqc = npg_warehouse::loader::illumina::fqc->new(digests => {
    'digest1' => $c1, 'digest2' => $c2, 'digest3' => $c3
  });
  $fqc->retrieve();
  my $outcomes = $fqc->retrive_outcomes('digest1');
  my $outcome  = $fqc->retrive_seq_outcome('22:1');

=head1 DESCRIPTION

A retriever for overall, sequencing, library and user QC outcomes to
be loaded to the warehouse table. Only final outcomes are retrieved.

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
  my @digests = keys %{$self->digests};

  if (@digests == 0) {
    return $outcomes;
  }

  my @qc_outcomes_tables = ($SEQ_OUTCOME_ENT, $LIB_OUTCOME_ENT, $UQC_OUTCOME_ENT);

  my $rs = $self->schema_qc->resultset('SeqComposition');
  my @rows = grep { defined } map {
    $rs->search({digest => $_}, {prefetch => \@qc_outcomes_tables})->next()
  } @digests;

  my $lanes_from_components = sub {
    return map { join q[:], $_->id_run, $_->position } @_;
  };
  my $is_single_lane = sub {
    my @components = @_;
    return (@components == 1 && !defined $components[0]->tag_index) ? 1 : 0;
  };

  my $lib_outcomes_decomposed = {};

  for my $crow (@rows) {
    my $digest = $crow->digest;
    for my $related_outcome (@qc_outcomes_tables) {
      my $orow = $crow->$related_outcome;
      # It's a pass or a fail, or, for library QC, a final undefined.
      if ($orow) {
        if (($related_outcome ne $UQC_OUTCOME_ENT) && !$orow->has_final_outcome) {
          next;
        }
        my $o = $orow->is_accepted ? 1 : ($orow->is_rejected ? 0 : undef);
        $outcomes->{$digest}->{$related_outcome} = $o;
        if ( (defined $o) && ($related_outcome eq $LIB_OUTCOME_ENT) &&
            ($self->digests->{$digest}->num_components() > 1) ) {
          $self->_cache_lib_outcome($lib_outcomes_decomposed, $digest, $o);
        }
      }
    }
    my @components = map {$_->seq_component}
                     $crow->seq_component_compositions->all();
    $outcomes->{$digest}->{$IS_SINGLE_LANE}  = $is_single_lane->(@components);
    $outcomes->{$digest}->{$COMPONENT_LANES} = [$lanes_from_components->(@components)];
  }

  foreach my $digest (@digests) {
    if (!exists $outcomes->{$digest}) {
      #####
      # Either this entity has not been qc-ed or the outcome is not final yet or
      # it will never be (has never been) qc-ed. For example, for a Standard workflow
      # of a NovaSeq run, library QC is performed on a level of merged entities, i.e.
      # individual plexes are never qc-ed.
      my @components  = $self->digests->{$digest}->components_list();
      $outcomes->{$digest}->{$IS_SINGLE_LANE}  = $is_single_lane->(@components);
      $outcomes->{$digest}->{$COMPONENT_LANES} = [$lanes_from_components->(@components)];
      #####
      # If the merged entity for which this entity is a component has a final library
      # outcome, we will assign this outcome to this component's library outcome.
      if (defined $lib_outcomes_decomposed->{$digest}) {
        $outcomes->{$digest}->{$LIB_OUTCOME_ENT} = $lib_outcomes_decomposed->{$digest};
      }
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
leads to an overall pass unless the library QC outcome is a fail; then the
overall value is a fail. It would've been better to set the overall value to
undefined if the library QC value is undefined. However, we have lots of legacy
data where library QC values were never set and the overall value was previously
reported as a pass. Since some customers rely on this, we cannot change this
feature, however imperfect it is.

Sequencing QC outcome for multi-component entities is composed from sequencing QC
outcomes for individual lanes. If all of them are a pass, the value is a pass, if
one of them is a fail, the value is a fail and if none are failed, but one of them
is undefined, the value is undefined.

If a library QC outcome for an individual plex or lane does not exist, but this
plex/lane is a component of a merged entity that has library QC outcome, the
latter outcome is assigned to the individual plex or lane.

User QC outcome has no dependency on either sequencing or library QC outcome.
It's purpose is to flag data as usable by end user when manual QC process resulted
in an overall fail and other way around. Therefore, the user QC outcome, when
defined either as a pass or a fail, overwrites the overall QC outcome for a product.
User QC outcome is applicable only to a product and affects only the overall QC
outcome for this product. The notion of preliminary or final outcome is not
applicable to use QC outcome, it's value is subject to change at any post-archival.

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
    $h->{$COL_NAME_QC_LIB}  = $outcome->{$LIB_OUTCOME_ENT};
    $h->{$COL_NAME_QC_SEQ}  = $outcome->{$SEQ_OUTCOME_ENT};

    if (!defined $h->{$COL_NAME_QC_SEQ} && !$outcome->{$IS_SINGLE_LANE}) {
      $h->{$COL_NAME_QC_SEQ} = $self->_get_lane_seq_outcome($digest);
    }

    if (defined $outcome->{$UQC_OUTCOME_ENT}) {
      $h->{$COL_NAME_QC_USER} = $outcome->{$UQC_OUTCOME_ENT};
      $h->{$COL_NAME_QC} = $h->{$COL_NAME_QC_USER};
    } else {
      $h->{$COL_NAME_QC} = $h->{$COL_NAME_QC_SEQ};
      if ($h->{$COL_NAME_QC} && defined $h->{$COL_NAME_QC_LIB}) {
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

sub _cache_lib_outcome {
  my ($self, $lib_outcomes_decomposed, $digest, $o) = @_;

  #####
  # We will decompose the multi-component compositions, create a single-
  # component composition from each component and cache the lib outcome
  # for the original composition against each of these new compositions
  foreach my $component ($self->digests->{$digest}->components_list()) {
    my $one_component_composition =
      npg_tracking::glossary::composition->new(components => [$component]);
    my $oc_digest = $one_component_composition->digest;
    if (!exists $lib_outcomes_decomposed->{$oc_digest}) {
      $lib_outcomes_decomposed->{$oc_digest} = $o;
    } else {
      if ($lib_outcomes_decomposed->{$oc_digest} != $o) {
        # This scenario is unlikely, but just in case...
        carp 'Conflicting inferred outcomes for ' . $one_component_composition->freeze();
        # Set ambigious results as fails
        $lib_outcomes_decomposed->{$oc_digest} = 0;
      }
    }
  }

 return;
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

  foreach my $lane_key (@{$self->_outcomes->{$digest}->{$COMPONENT_LANES}}) {
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

=item npg_tracking::glossary::composition

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Marina Gourtovaia

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2015,2017,2018,2019,2020 Genome Research Ltd.

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
