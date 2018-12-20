package npg_warehouse::loader::npg;

use Carp;
use Moose;
use MooseX::StrictConstructor;
use List::MoreUtils qw/none/;
use npg_tracking::Schema;

with 'npg_tracking::glossary::run';

our $VERSION = '0';

=head1 NAME

npg_warehouse::loader::npg

=head1 SYNOPSIS

=head1 DESCRIPTION

A retriever for NPG tracking data that is needed by the npg warehouse loader

=head1 SUBROUTINES/METHODS

=head2 verbose

Verbose flag

=cut
has 'verbose'      => ( isa        => 'Bool',
                        is         => 'ro',
                        required   => 0,
                        default    => 0,
                      );

=head2 schema_npg

DBIx schema object for the NPG tracking database

=cut
has 'schema_npg' =>  ( isa        => 'npg_tracking::Schema',
                       is         => 'ro',
                       required   => 0,
                       lazy_build => 1,
                     );
sub _build_schema_npg {
    return npg_tracking::Schema->connect();
}

=head2 id_run

Run id, optional attribute.

=cut
has '+id_run'   =>        (required        => 0,);

=head2 run_ready2load

A boolean test returning true if the run can be loaded and false
if it's too early (bases on run status) to load the run

=cut
sub run_ready2load {
    my $self = shift;
    if (!$self->id_run) {
        croak 'Need run id';
    }
    my $status_desc = $self->schema_npg()->resultset('Run')->find($self->id_run)->current_run_status_description() || q[];
    if ($self->verbose) {
        warn "Run status is '$status_desc'\n";
    }
    return none {$status_desc eq $_} ('run pending', 'run in progress', 'run on hold');
}

=head2 run_is_paired_read

Returns 1 if this run is paired, 0 otherwise.

=cut
sub run_is_paired_read {
    my $self = shift;
    my $count =  $self->schema_npg->resultset('TagRun')->search(
        { 'me.id_run' => $self->id_run, 'tag.tag' => 'paired_read', },
        { prefetch => 'tag',},
    )->count;
    return $count > 0 ? 1 : 0;
}

=head2 run_is_indexed

Returns 1 if this run is indexed, 0 otherwise.

=cut
sub run_is_indexed {
    my $self = shift;
    my $count =  $self->schema_npg->resultset('TagRun')->search(
        { 'me.id_run' => $self->id_run, 'tag.tag' => 'multiplex', },
        { prefetch => 'tag',},
    )->count;
    return $count > 0 ? 1 : 0;
}

=head2 dates

Returns dates for run pending run complete and qc complete

=cut
sub dates {
    my $self = shift;

    # Get the earliest run pending status date and
    # the latest run complete and qc complete date

    my $dates = {};
    my $rs = $self->schema_npg->resultset('RunStatus')->search(
       { 'me.id_run' => $self->id_run, 'run_status_dict.description' =>  'run pending', },
       {
           prefetch => 'run_status_dict',
           order_by => [{-asc => q[me.date]}],
       },
    )->next;
    if ($rs) {
	$dates->{run_pending} = $rs->date;
    }

    $rs = $self->schema_npg->resultset('RunStatus')->search(
       { 'me.id_run' => $self->id_run, 'run_status_dict.description' => 'qc complete' },
       {
           prefetch => 'run_status_dict',
           order_by => [{-desc => q[me.date]}],
       },
    )->next;

    if ($rs) {
        $dates->{qc_complete} =  $rs->date;
    }

    $rs = $self->schema_npg->resultset('RunStatus')->search(
       { 'me.id_run' => $self->id_run, 'run_status_dict.description' => 'run complete' },
       {
           prefetch => 'run_status_dict',
           order_by => [{-desc => q[me.date]}],
       },
    )->next;

    if ($rs) {
        $dates->{run_complete} =  $rs->date;
    }

    return $dates;
}

=head2 run_is_cancelled

Returns 1 if the run is cancelled, 0 otherwise

=cut
sub run_is_cancelled {
    my $self = shift;
    ##no critic (ProhibitNoisyQuotes)
    my $count = $self->schema_npg->resultset('RunStatus')->search(
       { 'me.id_run' => $self->id_run, 'me.iscurrent' => 1, 'run_status_dict.description' => {'=', ['run cancelled', 'data discarded']}, },
       { prefetch => 'run_status_dict',},
    )->count;
    return $count > 0 ? 1 : 0;
}

=head2 instrument_info

Returns information about an instrument on which sequencing was performed
and the way it was performed.

=cut
sub instrument_info {
    my $self = shift;
    my $info = {};
    my $row = $self->schema_npg->resultset('Run')->find(
               {id_run => $self->id_run},
               { prefetch => ['instrument', 'instrument_format'],},
    );
    if ($row) {
        $info->{'instrument_name'}  = $row->instrument->name;
        $info->{'instrument_model'} = $row->instrument_format->model;
        $info->{'instrument_side'}  = $row->instrument_side;
        $info->{'workflow_type'}    = $row->workflow_type;
    }
    return $info;
}

__PACKAGE__->meta->make_immutable;

1;

__END__


=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Carp

=item Moose

=item MooseX::StrictConstructor

=item List::MoreUtils

=item npg_tracking::Schema

=item npg_tracking::glossary::run

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Andy Brown and Marina Gourtovaia

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
