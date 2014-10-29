package npg_warehouse::loader::qc;

use Carp;
use Moose;
use Math::Round qw / round /;
use Readonly;

use npg_qc::Schema;

our $VERSION = '0';

Readonly::Array   my @CLUSTER_DENSITY_COLUMNS => qw/ raw_cluster_density 
                                                    pf_cluster_density  /;
Readonly::Hash    my %QUALITIES => { 'thirty' => 'q30', 'forty' => 'q40',};
Readonly::Scalar  my $THOUSAND  => 1000;

=head1 NAME

npg_warehouse::loader::qc

=head1 SYNOPSIS

=head1 DESCRIPTION

A retriever for NPG QC data to be loaded to the warehouse table

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

DBIx schema object for the NPGQC database

=cut
has 'schema_qc' =>   ( isa        => 'npg_qc::Schema',
                       is         => 'ro',
                       required   => 0,
                       lazy_build => 1,
                     );
sub _build_schema_qc {
    my $self = shift;
    my $schema = npg_qc::Schema->connect();
    if($self->verbose) {
        carp q[Connected to the qc db, schema object ] . $schema;
    }
    return $schema;
}

=head2 reverse_end_index

reverse_end_index

=cut
has 'reverse_end_index' => ( isa        => 'Int',
                             is         => 'ro',
                             required   => 1,
                           );

=head2 plex_key

Name of the key to use in data structures for plex data.

=cut
has 'plex_key' =>   ( isa             => 'Str',
                      is              => 'ro',
                      required        => 1,
		    );


=head2 retrieve_cluster_density

Returns a hash containing per lane cluster densities

=cut
sub retrieve_cluster_density {
    my ($self, $id_run) = @_;

    if (!defined $id_run) {
        croak 'Run id argument should be set';
    }

    my $density = {};
    my $rs = $self->schema_qc->resultset('ClusterDensity')->search(
        { id_run => $id_run,},
        { columns => [ qw/id_run position is_pf p50/],},
    );

    while (my $row = $rs->next) {
        if(defined $row->p50) {
            my $cluster_density_column = $row->is_pf ? $CLUSTER_DENSITY_COLUMNS[1] : $CLUSTER_DENSITY_COLUMNS[0];
            $density->{$row->position}->{$cluster_density_column} = $row->p50;
	}
    }
    return $density;
}


=head2 retrieve_summary

Returns a hash containing per lane/end NPG QC information

=cut
sub retrieve_summary {
    my ($self, $id_run, $end, $has_two_runfolders) = @_;

    if (!defined $id_run) {
        croak 'Run id argument should be set';
    }
    if (!defined $end) {
        croak 'End argument should be set';
    }
    if (!defined $has_two_runfolders) {
        croak 'Two run folders flag argument should be set';
    }

    my $ends = [$end];
    my $lanes = {};
    if (!$has_two_runfolders) {
        if ($end == $self->reverse_end_index) {
            croak qq[Reverse end index AND run with one runfolder for run $id_run];
	}
        push @{$ends}, $self->reverse_end_index;
    }

    my $rs_all = $self->schema_qc->resultset('CacheQuery')->search(
             {
                 id_run     => $id_run,
	         end        => $ends,
                 is_current => 1,
                 type       => 'lane_summary',
             },
             {
                 columns => [ qw/results/],
             },
    					                           );
    my $rs;
    while ($rs = $rs_all->next) {
        my $result = $rs->results;
        if ($result) {
            my $rows_ref;
            my $semi_colon_count = $result =~ tr/;/;/;
            if ($semi_colon_count == 1 && $result =~ /[$]rows_ref[ ]=[ ]\[\{.*?\}\];\z/xms) {
                eval $result; ## no critic (ProhibitStringyEval,RequireCheckingReturnValueOfEval)
            } else {
                croak 'Too many statements in returned code: ' . $result;
            }

            foreach my $lane_hash (@{$rows_ref}) {
                $lanes->{$lane_hash->{lane}}->{$lane_hash->{end}} = $lane_hash;
            }
	}
    }
    return $lanes;
}

=head2 retrieve_yields

Returns a hash containing per lane  q30 base counts

=cut
sub retrieve_yields {
    my ($self, $id_run) = @_;

    if (!defined $id_run) {
        croak 'Run id argument should be set';
    }
    my $lanes = {};

    my $rs_all = $self->schema_qc->resultset('Fastqcheck')->search(
             {
                 id_run     => $id_run,
                 split      => 'none',
             },
             {
                 columns => [ qw/section position tag_index thirty forty/ ],
             },
    					                           );
    while (my $r= $rs_all->next) {
      my $read = $r->section;
      if ($read =~ /forward|reverse/smx) {
        foreach my $q (keys %QUALITIES) {
          my $value = $r->$q;
          if (defined $value) {
            if ($value) {
              $value = round($value/$THOUSAND);
	    }
            my $column_name = join q[_], $QUALITIES{$q}, 'yield_kb', $read, 'read';
            my $tag_index = $r->tag_index;
            my $position = $r->position;
            if(!defined $tag_index) {
              $lanes->{$position}->{$column_name} = $value;
            } else {
              $lanes->{$position}->{$self->plex_key}->{$tag_index}->{$column_name} = $value;
	    }
	  }
	}
      }
    }

    return $lanes;
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

=item Math::Round qw/round/

=item npg_qc::Schema

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Andy Brown and Marina Gourtovaia

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2014 Genome Research Limited

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
