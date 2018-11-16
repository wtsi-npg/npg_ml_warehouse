package npg_warehouse::loader::fqc;

use Carp;
use Moose;
use MooseX::StrictConstructor;
use Readonly;

use npg_tracking::glossary::rpt;
use npg_qc::Schema;
use npg_qc::mqc::outcomes;

our $VERSION = '0';

Readonly::Scalar my $COL_NAME_QC     => 'qc';
Readonly::Scalar my $COL_NAME_QC_SEQ => $COL_NAME_QC . '_seq';
Readonly::Scalar my $COL_NAME_QC_LIB => $COL_NAME_QC . '_lib';

=head1 NAME

npg_warehouse::loader::fqc

=head1 SYNOPSIS

=head1 DESCRIPTION

A retriever for overall, sequencing and library QC outcomes to be
loaded to the warehouse table.

=head1 SUBROUTINES/METHODS

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

has '_retriever' =>   ( isa        => 'npg_qc::mqc::outcomes',
                        is         => 'bare',
                        required   => 0,
                        lazy_build => 1,
		        handles    => {
                          '_get_outcomes' => 'get'
		        },
                      );
sub _build__retriever {
  my $self = shift;
  return npg_qc::mqc::outcomes->new(qc_schema  => $self->schema_qc);
}

=head2 retrieve_outcomes

Retrieves qc outcomes for an entity defined by the argument composition
as a hash in the format expected by the warehouse loader (keys as qc,
qc_lib and qc_seq and values as 0, 1 or undefined).

  my $qc_outcomes = $obj->retrieve_outcomes($composition);

Applies certain rules computing the overall QC outcome from outcomes
of the library and sequencing QC. Non-final QC outcomes are equivalent
to outcome not being defined. A fail on sequencing QC leads to the
overall fail. A pass on sequencing QC is overwritten by the library
QC outcome, meaning that the overall QC would be undefined if the
library QC value is undefined. Sequencing QC outcome for multi-component
entities is composed from sequencing QC outcomes for individual lanes.
If all of them pass, the value is a pass, if one of then fail, the value
is a fail and if none are failed, but one of them is undefined, the value
is undefined.
  
=cut

sub retrieve_outcomes {
  my ($self, $composition) = @_;

  if (!$composition) {
    croak 'Composition object is missing';
  }

  my $h = {};
  for my $name (($COL_NAME_QC, $COL_NAME_QC_SEQ, $COL_NAME_QC_LIB)) {
    $h->{$name} = undef;
  }

  my $rpt_list = $composition->freeze2rpt();
  my $outcomes = $self->_get_outcomes([$rpt_list]);
  my $lib_qc = $outcomes->{'lib'}->{$rpt_list}->{'mqc_outcome'};
  $h->{$COL_NAME_QC_LIB} = $self->_outcome_desc2wh_value('lib', $lib_qc);

  my @lane_rpts = map { _lane_rpt_from_rpt($_->freeze2rpt()) }
                  $composition->components_list;
  my $seq_qc;
  if (scalar @lane_rpts == 1) {
    $seq_qc = $outcomes->{'seq'}->{$lane_rpts[0]}->{'mqc_outcome'};
    $seq_qc = $self->_outcome_desc2wh_value('seq', $seq_qc);
  } else {
    my $lo = $self->_get_outcomes(\@lane_rpts);
    my @lane_outcomes =
      map { $self->_outcome_desc2wh_value('seq', $_) }
      map { $lo->{$_}->{'mqc_outcome'} }
      @lane_rpts;
    if (any { defined $_ && $_ == 0 } @lane_outcomes) {
      $seq_qc = 0;
    } elsif (none { !defined } @lane_outcomes) {
      $seq_qc = 1;
    }
  }

  $h->{$COL_NAME_QC_SEQ} = $seq_qc;

  if (!defined $h->{$COL_NAME_QC_SEQ} && defined $h->{$COL_NAME_QC_LIB}) {
    croak 'Inconsistent qc outcomes';
  }

  $h->{$COL_NAME_QC} = $h->{$COL_NAME_QC_SEQ};
  if (defined $h->{$COL_NAME_QC_LIB}) {
    if (!$h->{$COL_NAME_QC_LIB}) {
      $h->{$COL_NAME_QC} = 0;
    }
  } else {
    # No overwrite if the query was about a single lane
    if ((scalar @lane_rpts > 1) || ($lane_rpts[0] ne $rpt_list)) {
      $h->{$COL_NAME_QC} = undef;
    }
  }

  return $h;
}

=head2 retrieve_lane_outcome

Retrieves outcome for a single lane as 0, 1 or undefined. Takes
composition object for a lane as an argument. No check is
made to ensure that the composition is for a lane.

 my $outcome = $obj->retrieve_lane_outcome($composition);

=cut

sub retrieve_lane_outcome {
  my ($self, $composition) = @_;

  if (!$composition) {
    croak 'Composition object is missing';
  }
  return $self->_seq_outcome($composition->freeze2rpt());
}

sub _lane_rpt_from_rpt {
  my $rpt = shift;
  my $h = npg_tracking::glossary::rpt->inflate_rpt($rpt);
  delete $h->{'tag_index'};
  return npg_tracking::glossary::rpt->deflate_rpt($h);
}

sub _seq_outcome {
  my ($self, $rpt) = @_;
  my $seq_qc = $self->_get_outcomes([$rpt])
                    ->{'seq'}->{$rpt}->{'mqc_outcome'};
  if ($seq_qc) {
    $seq_qc = $self->_outcome_desc2wh_value('seq', $seq_qc);
  }
  return $seq_qc;
}

sub _outcome_desc2wh_value {
  my ($self, $qc_type, $desc) = @_;

  my $outcome;
  if ($desc) {
    $qc_type ||= q[];
    my $rs_name = $qc_type eq 'lib' ? 'MqcLibraryOutcomeDict' :
                 ($qc_type eq 'seq' ? 'MqcOutcomeDict'
                 : croak "Unknown qc type $qc_type");
    my $row = $self->schema_qc->resultset($rs_name)
                   ->search({'short_desc' => $desc})->next;
    if (!$row) {
      croak "Invalid qc outcome description $desc";
    }
    if ($row->is_final_outcome) {
      if ($row->is_accepted) {
        $outcome = 1;
      } elsif ($row->is_rejected) {
        $outcome = 0;
      }
    }
  }

  return $outcome;
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

=item npg_tracking::glossary::rpt

=item npg_qc::mqc::outcomes

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
