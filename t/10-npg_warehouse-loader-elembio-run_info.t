use strict;
use warnings;
use Perl6::Slurp;
use File::Temp qw/tempdir/;
use File::Copy qw/cp/;
use DateTime;
use Test::More tests => 3;
use Test::Exception;

use_ok('npg_warehouse::loader::elembio::run_info');

my $schema_wh = Moose::Meta::Class->create_anon_class(
    roles => [qw/npg_testing::db/]
)->new_object({})->create_test_db(q[WTSI::DNAP::Warehouse::Schema]);

subtest 'construct object, test for errors when loading' => sub {
  plan tests => 6;

  my $path = 't/data';
  my $loader = npg_warehouse::loader::elembio::run_info->new(
    schema_wh => $schema_wh, runfolder_path => $path
  );
  isa_ok ($loader, 'npg_warehouse::loader::elembio::run_info');
  is ($loader->run_folder, 'data', 'run folder name');
  is ($loader->npg_tracking_schema, undef,
    'tracking database access is blocked');
  throws_ok { $loader->load() }
     qr/File t\/data\/RunParameters\.json does not exist/,
    'error if run params file does not exist';

  $path = 't/some';
  $loader = npg_warehouse::loader::elembio::run_info->new(
    schema_wh => $schema_wh, runfolder_path => $path
  );
  is ($loader->run_folder, 'some', "run folder 'some', path does not exist");
  throws_ok { $loader->load() } qr/Run folder path t\/some does not exist/,
    'run time error if the run folder path does not exist';
}; 

subtest 'load the data' => sub {
  plan tests => 26;

  my $rs = $schema_wh->resultset('EseqRun');
  is ($rs->count(), 0, 'eseq_run table is initially empty');

  my $rf_name = '20250127_AV244103_NT1850075L';
  my $path = "t/data/elembio/$rf_name";
  my $stats_file = "$path/AvitiRunStats.json";
  my $params_file = "$path/RunParameters.json";

  my $loader = npg_warehouse::loader::elembio::run_info->new(
    schema_wh => $schema_wh, runfolder_path => $path
  );
  is ($loader->run_folder, $rf_name, 'run folder name');
  lives_ok { $loader->load() } 'no error loading files';
  is ($rs->count(), 1, 'eseq_run table now has 1 row');
  lives_ok { $loader->load() } 'no error reloading files';
  is ($rs->count(), 1, 'eseq_run table still has 1 row');
  
  my $row = $rs->next();
  is ($row->folder_name, $rf_name, 'run folder name is saved correctly');
  ok (!defined $row->run_stats, 'run statistics file is not saved');
  is ($row->run_parameters, slurp($params_file),
    'run parameters file is saved correctly');

  ############
  # Model a successfully completed run.
  ############
  my $tdir = tempdir(CLEANUP => 1);
  $rf_name = '20250128_AV244103_NT1850075L';
  my $new_dir = "$tdir/elembio/$rf_name";
  mkdir "$tdir/elembio";
  mkdir $new_dir;

  # Gradually build a collection of files we expect to find in a run folder.
  cp $params_file,"$new_dir/RunParameters.json"; # step 1

  $loader = npg_warehouse::loader::elembio::run_info->new(
    schema_wh => $schema_wh, runfolder_path => $new_dir
  );
  is ($loader->load(), 1, 'loading worked');
  is ($rs->count(), 2, 'eseq_run table has 2 rows');
  $rs = $rs->search({folder_name => $rf_name});
  is ($rs->count(), 1, 'one row with a new run folder name found');
  $row = $rs->next();
  ok ($row->run_parameters, 'run params file content is loaded');
  is ($row->date_completed, undef, 'run completion date is undefined');
  is ($row->outcome, undef, 'run outcome is undefined');
  is ($row->run_name, 'NT1850075L', 'run name is correct');
  is ($row->flowcell_id, '2427452508', 'flowcell ID is correct');
  is ($row->date_started,  '2025-01-27T15:41:06', 'startd date is correct');

  `cp $path/RunUploaded.json $new_dir/RunUploaded.json`; # step 3
  my $time = DateTime->new(
    year => 2025,month => 1,day => 29,hour => 13,minute => 30,second => 0
  )->epoch;
  utime $time, $time, "$new_dir/RunUploaded.json"; # set the timestamp
  $loader = npg_warehouse::loader::elembio::run_info->new(
    schema_wh => $schema_wh, runfolder_path => $new_dir
  );  
  is ($loader->load(), 1, 'loading worked');
  $row = $rs->search({folder_name => $rf_name})->next();
  is ($row->outcome, 'OutcomeCompleted', 'run outcome is correct');
  is ($row->date_completed, '2025-01-29T13:30:00', 'run completion date is correct');

  ############ 
  # Model a failed run.
  # A failed run has RunParameters.json and RunUploaded.json files in its
  # run folder. Might have any number of other files. RunUploaded.json file
  # contains a flag indicating a failure.
  ############
  $rf_name = '20240216_AV234003_16feb_sge_1x300';
  $loader = npg_warehouse::loader::elembio::run_info->new(
    schema_wh => $schema_wh,
    runfolder_path => "t/data/elembio/$rf_name"
  );
  is ($loader->load(), 1, 'loading worked');
  my $rf_rs = $schema_wh->resultset('EseqRun')->search({folder_name => $rf_name});
  is ($rf_rs->count(), 1, 'a record is created');
  is ($rf_rs->next()->outcome, 'OutcomeFailed', 'run outcome is correct');

  ############ 
  # Model a 'technical' run.
  # Normally only RunParameters.json is present in a run folder, no data,
  # no RunUploaded.json. FlowcellID value in RunParameters.json is unset.
  ############
  $rf_name = '20231218_AV234003_FluidicsRun_1_A';
  $loader = npg_warehouse::loader::elembio::run_info->new(
    schema_wh => $schema_wh,
    runfolder_path => "t/data/elembio/$rf_name"
  );
  is ($loader->load(), 0, 'loading was skipped');
  is ($schema_wh->resultset('EseqRun')->search({folder_name => $rf_name})->count(),
    0, 'a record for a technical run is not present');
};
