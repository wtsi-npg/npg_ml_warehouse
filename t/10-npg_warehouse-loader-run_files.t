use strict;
use warnings;
use Test::More tests => 4;
use Test::Exception;
use File::Temp qw/tempdir/;
use File::Copy;
use File::Slurp;
use Cwd;
use Moose::Meta::Class;
use Log::Log4perl qw/:levels/;

my $tdir = tempdir(UNLINK => 1);
my $layout = '%d %-5p %c - %m%n';
my $level  = $INFO;
Log::Log4perl->easy_init({layout => $layout,
                          level  => $level,
                          file   => "$tdir/logfile",
                          utf8   => 1});

use_ok('npg_warehouse::loader::run_files');

my $util = Moose::Meta::Class->create_anon_class(
             roles => [qw/npg_testing::db/])->new_object({});
my $schema  = $util->create_test_db(q[WTSI::DNAP::Warehouse::Schema]);

subtest 'create an object' => sub {
  plan tests => 4;

  throws_ok { npg_warehouse::loader::run_files->new(
    schema_wh => $schema, path_glob =>q[t]) }
    qr/is required/, 'error if id_run is not given';
  throws_ok { npg_warehouse::loader::run_files->new(
    schema_wh => $schema, id_run => 45) }
    qr/is required/, 'error if path glob is not given';
  my $loader;
  lives_ok { $loader = npg_warehouse::loader::run_files->new(
    schema_wh => $schema, path_glob =>q[t], id_run => 45) }
    'no error if all required attributes are supplied';
  isa_ok ($loader, 'npg_warehouse::loader::run_files');
};

subtest 'find file to load' => sub {
  plan tests => 7;

  my $input = {schema_wh => $schema, id_run => 45, path_glob =>q[]};  
  my $loader = npg_warehouse::loader::run_files->new($input);
  throws_ok { $loader->_file_path() }
    qr/Non-empty path glob is required/,
    'path glob cannot be represented by an empty string';
  
  $input->{path_glob} = "$tdir/*.xml";
  $loader = npg_warehouse::loader::run_files->new($input);
  throws_ok { $loader->_file_path() }
    qr/No files found/, 'error if no files are found';
  
  mkdir "$tdir/some.xml";
  $loader = npg_warehouse::loader::run_files->new($input);
  throws_ok { $loader->_file_path() }
    qr/No files found/, 'error if no files are found';

  my $name = 'RunParameters_NovaSeq.xml';
  my $destination = "$tdir/$name";
  copy("t/data/runfolders/run_params/$name", $destination);
  $input->{path_glob} = "$tdir/{r,R}un*.xml";
  $loader = npg_warehouse::loader::run_files->new($input);
  is ($loader->_file_path(), $destination, 'correct file path');

  # This type of input will be used by the pipeline
  $input->{path_glob} = "$tdir/{r,R}unParameters_NovaSeq.xml";
  $loader = npg_warehouse::loader::run_files->new($input);
  is ($loader->_file_path(), $destination, 'correct file path');
  
  $input->{path_glob} = "$tdir/some.xml/../{r,R}un*.xml";
  $loader = npg_warehouse::loader::run_files->new($input);
  is ($loader->_file_path(), $destination, 'relative file path is resolved');

  copy("t/data/runfolders/run_params/$name", "$tdir/RunParameters.xml");
  $input->{path_glob} = "$tdir/{r,R}un*.xml";
  $loader = npg_warehouse::loader::run_files->new($input);
  throws_ok { $loader->_file_path() }
    qr/Multiple files found/, 'error when multiple files are found';
};

subtest 'load the data' => sub {
  plan tests => 14;

  my $dir = 't/data/runfolders/run_params';
  my $file = join q[/], getcwd(), $dir, 'RunParameters_NovaSeq.xml';

  # Use relative path and a glob.
  my $loader = npg_warehouse::loader::run_files->new(
    id_run => 45,
    path_glob => "$dir/{r,R}unParameters_Nova*.xml",
    schema_wh => $schema
  );
  is ($loader->load(), $file, 'correct absolute file path is returned');

  # Use a full relative path.
  $loader = npg_warehouse::loader::run_files->new(
    id_run => 45,
    path_glob => "$dir/RunParameters_NovaSeq.xml",
    schema_wh => $schema
  );
  is ($loader->load(), $file, 'correct absolute file path is returned');

  # Use an absulute path.
  $loader = npg_warehouse::loader::run_files->new(
    id_run => 45,
    path_glob => $file,
    schema_wh => $schema
  );
  is ($loader->load(), $file, 'correct absolute file path is returned');

  # Inspect the loaded data.
  my $run_row = $schema->resultset('IseqRun')->find(45);
  ok ($run_row, 'run row was created');
  my $row = $schema->resultset('IseqRunInfo')->find(45);
  ok ($row, 'the row for the param file was created');
  ok ($row->run_parameters_xml, 'file contents is loaded');
  is ($run_row->rp__read1_number_of_cycles, 221, 'read1_number_of_cycles');
  is ($run_row->rp__read2_number_of_cycles, 201, 'read2_number_of_cycles');
  is ($run_row->rp__flow_cell_mode, 'SP', 'flow_cell_mode');
  is ($run_row->rp__workflow_type, 'NovaSeqXp', 'workflow_type');
  is ($run_row->rp__flow_cell_consumable_version, '1', 'flow_cell_consumable_version');
  is ($run_row->rp__sbs_consumable_version, '3', 'sbs_consumable_version');

  $file = "$tdir/some.txt";
  write_file($file, qw/first_line second_line/);
  $loader = npg_warehouse::loader::run_files->new(
    id_run => 45,
    path_glob => $file,
    schema_wh => $schema
  );
  throws_ok { $loader->load() }
    qr/File $file is not an Illumina run params file/,
    'error when the file is not the Illumina run params. file';

  write_file($file, q[]);
  throws_ok { $loader->load() }
    qr/File $file is empty/, 'error when the file is empty';
};

1;
