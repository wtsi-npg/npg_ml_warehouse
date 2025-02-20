package npg_warehouse::loader::elembio::run_info;

use Moose;
use MooseX::StrictConstructor;
use Carp;
use Readonly;
use Perl6::Slurp;
use JSON;
use DateTime;
use DateTime::Format::Strptime;
use Log::Log4perl qw(:easy :levels);

use npg_tracking::illumina::run::folder;
use WTSI::DNAP::Warehouse::Schema;

with qw/ MooseX::Getopt
         npg_tracking::illumina::run::folder /;

our $VERSION = '0';

Readonly::Scalar my $RUN_PARAMS_FILE_NAME => 'RunParameters.json';
Readonly::Array  my @FILE_NAMES => ($RUN_PARAMS_FILE_NAME, 'AvitiRunStats.json');
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

C<RunParameters.json> are C<ElembioRunStatus.json> files are uploaded to the
table as are, without any reductions.

This Moose class, via inheritance from C<npg_tracking::illumina::run::folder>,
has a number of attributes for accessing paths inside the run folder. These
attributes might be useful in future, if and when Elembio runs are registered
in the tracking database. At the time of writing only the methods documented
below are meaningful and safe to use.

Access to the run tracking database is blocked unless the C<npg_tracking_schema>
attribute is set by the caller. 

=head1 SUBROUTINES/METHODS

=head2 runfolder_path

Elembio run folder path, including the run folder name. Required.
Inherited from npg_tracking::illumina::run::folder

=head2 run_folder

Run folder name. Inherited from npg_tracking::illumina::run::folder

=cut

##### Customise inherited attributes

# Amend attributes which we do not want to show up as scripts' arguments.
my @no_script_arg_attrs =
  grep { $_ ne 'runfolder_path' }
  npg_tracking::illumina::run::folder->meta->get_attribute_list();
has [map {q[+] . $_ } @no_script_arg_attrs] => (metaclass => 'NoGetopt',);

# Amend the builder method for the npg_tracking_schema attribute.
# Blocks implicit access to the tracking database.
# Delete this method if/when the access is required.
sub _build_npg_tracking_schema {
  return;
}

has '+runfolder_path' => (required => 1,);

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

Loads the content of C<RunParameters.json> and C<ElembioRunStatus.json> files
to eseq_run table. Errors if the run folder does not exist or the expected
files are not found in the run folder.

=cut

sub load {
  my $self = shift;

  -d $self->runfolder_path or croak
    sprintf 'Run folder path %s does not exist', $self->runfolder_path;

  my $run_data = {};
  $run_data->{folder_name} = $self->run_folder;

  foreach my $file_name (@FILE_NAMES) {

    my ($column_name) = $file_name =~ /(Run [[:upper:]] [[:lower:]]+)[.]json\Z/smx;
    $column_name =~ s/Run/Run_/smx;
    $column_name = lc $column_name;
    my $file_path = join q[/], $self->runfolder_path, $file_name;
    if (-f $file_path) {
      $run_data->{$column_name} = slurp $file_path;
    } else {
      my $m = "File $file_path does not exist";
      ($file_name eq $RUN_PARAMS_FILE_NAME) ? croak $m : WARN($m);
    }

    if ($file_name eq $RUN_PARAMS_FILE_NAME) {
      $run_data->{flowcell_id} =
        _get_value_from_json($run_data->{$column_name}, 'FlowcellID');
      if (!$run_data->{flowcell_id}) { # A technical run, no data.
        WARN('Flowcell ID is not recorded, not loading');
        return 0;
      }
      $run_data->{run_name} =
        _get_value_from_json($run_data->{$column_name}, 'RunName');
      $run_data->{date_started} = _parse_date_string(
        _get_value_from_json($run_data->{$column_name}, 'Date'));
    }
  }

  my $run_uploaded_file = join q[/], $self->runfolder_path, $RUN_UPLOADED_FILE_NAME;
  if (-f $run_uploaded_file) {
    $run_data->{outcome} = _get_value_from_json(slurp($run_uploaded_file), 'outcome');
    $run_data->{date_completed} = _get_date_from_file_stats($run_uploaded_file);
  } else {
    INFO('The run has not completed yet');
  }

  $self->schema_wh->resultset($RUN_INFO_RS_NAME)->update_or_create($run_data);

  return 1;
}

sub _get_value_from_json {
  my ($json_string, $key) = @_;

  $json_string or croak 'Got an empty JSON string';
  $key or croak 'Got a empty key value';
  my $hash_data = decode_json $json_string;

  return $hash_data->{$key};
}

sub _parse_date_string {
  my $date_string = shift;

  my $date_obj;
  if ($date_string) {
    $date_obj = DateTime::Format::Strptime->new(
      pattern=>q[%Y-%m-%dT%T],
      strict=>1,
      on_error=>q[croak]
    )->parse_datetime($date_string); # 2023-12-19T13:31:17.461926614Z
  }
  return $date_obj;
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

=item DateTime::Format::Strptime

=item Log::Log4perl

=item WTSI::DNAP::Warehouse::Schema

=item npg_tracking::illumina::run::folder

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
