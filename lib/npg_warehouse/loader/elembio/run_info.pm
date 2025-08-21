package npg_warehouse::loader::elembio::run_info;

use Moose;
use MooseX::StrictConstructor;
use Carp;
use Readonly;
use Perl6::Slurp;
use JSON;
use DateTime;
use Log::Log4perl qw(:easy :levels);
use Try::Tiny;

use WTSI::DNAP::Warehouse::Schema;

extends 'Monitor::Elembio::RunParametersParser';

with 'MooseX::Getopt';

our $VERSION = '0';

Readonly::Scalar my $RUN_UPLOADED_FILE_NAME => 'RunUploaded.json';
Readonly::Scalar my $RUN_INFO_RS_NAME => 'EseqRun';

=head1 NAME

npg_warehouse::loader::elembio::run_info

=head1 SYNOPSIS
 
  my $path = 'some/path';
  npg_warehouse::loader::elembio::run_info->new(runfolder_path => $path)->load();

=head1 DESCRIPTION

Uploads (updates or inserts) manufacturer-supplied run information to
C<eseq_run> table of the ml warehouse database.

C<RunParameters.json> file is uploaded to the table as is, without any reductions.

=head1 SUBROUTINES/METHODS

=head2 runfolder_path

Elembio run folder path, including the run folder name. Required.
Inherited from Monitor::Elembio::RunParametersParser

=cut

##### Customise inherited attributes

# Amend attributes which we do not want to show up as scripts' arguments.
my @no_script_arg_attrs =
  grep { $_ ne 'runfolder_path' }
  Monitor::Elembio::RunParametersParser->meta->get_attribute_list();
has [map {q[+] . $_ } @no_script_arg_attrs] => (metaclass => 'NoGetopt',);

##### End of customisation

=head2 schema_wh

DBIx schema object for the warehouse database.

=cut

has 'schema_wh' => (
  isa        => 'WTSI::DNAP::Warehouse::Schema',
  metaclass  => 'NoGetopt',
  is         => 'ro',
  required   => 0,
  lazy_build => 1,
);
sub _build_schema_wh {
  return WTSI::DNAP::Warehouse::Schema->connect();
}

=head2 load

Loads the content of C<RunParameters.json> file to eseq_run table.
Errors if the run folder does not exist or the expected files are not
found in the run folder.

=cut

sub load {
  my $self = shift;

  my $flowcell_id;
  try {
    $flowcell_id = $self->flowcell_id;
  } catch {
    WARN("Error retrieving flowcell id: $_");
    WARN('Not loading data for run folder ' . $self->runfolder_path);
  };
  $flowcell_id || return 0;

  my $run_data = {};
  $run_data->{folder_name} = $self->folder_name;
  $run_data->{flowcell_id} = $flowcell_id;
  $run_data->{run_name} = $self->run_name;
  $run_data->{date_started} = $self->date_created;
  $run_data->{run_parameters} = slurp $self->runparams_path;

  my $run_uploaded_file = join q[/], $self->runfolder_path, $RUN_UPLOADED_FILE_NAME;
  if (-f $run_uploaded_file) {
    my $file_content = slurp $run_uploaded_file;
    if ($file_content) {
      my $hash_data = decode_json $file_content;
      $run_data->{outcome} = $hash_data->{'outcome'};
    } else {
      WARN "$run_uploaded_file file is empty";
    }
    $run_data->{date_completed} = _get_date_from_file_stats($run_uploaded_file);
  } else {
    INFO('The run has not completed yet');
  }

  $self->schema_wh->resultset($RUN_INFO_RS_NAME)->update_or_create($run_data);

  return 1;
}

sub _get_date_from_file_stats {
  my $file_path = shift;
  ## no critic (ValuesAndExpressions::ProhibitMagicNumbers)
  return DateTime->from_epoch((stat $file_path)[9]);
}

__PACKAGE__->meta->make_immutable;

1;

__END__


=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Readonly

=item Perl6::Slurp

=item Carp

=item Moose

=item MooseX::StrictConstructor

=item MooseX::Getopt

=item JSON

=item DateTime

=item Log::Log4perl

=item Try::Tiny

=item WTSI::DNAP::Warehouse::Schema

=item Monitor::Elembio::RunParametersParser

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Marina Gourtovaia

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2025 Genome Research Ltd.

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
