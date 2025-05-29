package npg_warehouse::loader::illumina::qc;

use Moose;
use namespace::autoclean;
use MooseX::StrictConstructor;
use Carp;
use Readonly;

use npg_qc::Schema;

our $VERSION = '0';

Readonly::Array my @CLUSTER_DENSITY_COLUMNS => qw/ raw_cluster_density 
                                                   pf_cluster_density  /;

=head1 NAME

npg_warehouse::loader::illumina::qc

=head1 SYNOPSIS

=head1 DESCRIPTION

A retriever for NPG QC data to be loaded to the warehouse.

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

=head2 retrieve_cluster_density

Returns a hash containing per lane cluster density
values.

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

=item namespace::autoclean

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
