package npg_warehouse::loader::autoqc_transform;

use Moose::Role;

our $VERSION = '0';

=head1 NAME

npg_warehouse::loader::autoqc_transform

=head1 SYNOPSIS

=head1 DESCRIPTION

This Moose role provides transform functions from autoqc result objects
to a hash with keys representing column names and corresponding values.
The functions might return an empty hash with no key-value pairs.

By using these function mlwh loader classes for different sequencing platforms
will ensure consistend data representation.

=head1 SUBROUTINES/METHODS

=head2 genotype_transform

A transform for C<npg_qc::autoqc::results::genotype> objests

=cut

sub genotype_transform {
    my ($self, $result) = @_;

    my $data = {};
    if (defined $result->sample_name_match) {
        # Probably, the data can be fixed instead of setting to 0 if false
        $data->{'genotype_sample_name_match'} = join q[/],
            $result->sample_name_match->{'match_count'}      || 0,
            $result->sample_name_match->{'common_snp_count'} || 0;
    }

    if (defined $result->sample_name_relaxed_match) {
        $data->{'genotype_sample_name_relaxed_match'} = join q[/],
            $result->sample_name_relaxed_match->{'match_count'},
            $result->sample_name_relaxed_match->{'common_snp_count'};
    }

    my $bam_gt_depths_string = $result->bam_gt_depths_string;
    if (defined $bam_gt_depths_string) {
        my $tot = 0;
        my $c = 0;
        for my $v (split /;/smx, $bam_gt_depths_string) {
            $tot += $v;
            $c++;
        }
        $data->{'genotype_mean_depth'} = sprintf '%.02f', ($tot / $c);
    }

    return $data;
}

1;

__END__

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Moose::Role

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Marina Gourtovaia

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2026 Genome Research Ltd.

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
