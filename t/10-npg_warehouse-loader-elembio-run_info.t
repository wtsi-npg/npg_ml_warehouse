use strict;
use warnings;
use Perl6::Slurp;
use File::Temp qw/tempdir/;
use File::Copy qw/cp/;
use DateTime;
use Test::More tests => 5;
use Test::Exception;

use_ok('npg_warehouse::loader::elembio::run_info');

my $schema_wh = Moose::Meta::Class->create_anon_class(
    roles => [qw/npg_testing::db/]
)->new_object({})->create_test_db(q[WTSI::DNAP::Warehouse::Schema]);

my $tdir = tempdir(CLEANUP => 1);
mkdir "$tdir/elembio";

subtest 'load the data for a sequencing run' => sub {
  plan tests => 27;

  my $rs = $schema_wh->resultset('EseqRun');
  is ($rs->count(), 0, 'eseq_run table is initially empty');

  my $rf_name = '20250127_AV244103_NT1850075L';
  my $path = "t/data/elembio/$rf_name";
  my $params_file = "$path/RunParameters.json";

  my $loader = npg_warehouse::loader::elembio::run_info->new(
    schema_wh => $schema_wh, runfolder_path => $path
  );
  is ($loader->folder_name, $rf_name, 'run folder name');
  is ($loader->runparams_path, $params_file, 'correct run params file path');

  lives_ok { $loader->load() } 'no error loading files';
  is ($rs->count(), 1, 'eseq_run table now has 1 row');
  lives_ok { $loader->load() } 'no error reloading files';
  is ($rs->count(), 1, 'eseq_run table still has 1 row');

  my $row = $rs->next();
  is ($row->folder_name, $rf_name, 'run folder name is saved correctly');
  is ($row->run_type, 'Sequencing', 'sequencing run type is recorded');
  ok (!defined $row->run_stats, 'run stats is not saved');
  is ($row->run_parameters, slurp($params_file),
    'run parameters file is saved correctly');

  ############
  # Model a successfully completed run.
  ############
  $rf_name = '20250225_AV244103_NT1850075L_NT1850808B_repeat3';
  my $new_dir = "$tdir/elembio/$rf_name";
  mkdir $new_dir;

  # Gradually build a collection of files we expect to find in a run folder.
  cp "t/data/elembio/$rf_name/RunParameters.json", "$new_dir/RunParameters.json";

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
  is ($row->run_name, 'NT1850075L_NT1850808B_repeat3', 'run name is correct');
  is ($row->flowcell_id, '2437688146', 'flowcell ID is correct');
  is ($row->run_type, 'Sequencing', 'sequencing run type is recorded');
  is ($row->date_started,  '2025-02-25T14:11:44', 'start date is correct');

  `cp $path/RunUploaded.json $new_dir/RunUploaded.json`;
  my $time = DateTime->new(
    year => 2025,month => 2,day => 28,hour => 13,minute => 30,second => 0
  )->epoch;
  utime $time, $time, "$new_dir/RunUploaded.json"; # set the timestamp
  $loader = npg_warehouse::loader::elembio::run_info->new(
    schema_wh => $schema_wh, runfolder_path => $new_dir
  );
  is ($loader->load(), 1, 'loading worked');
  $row = $rs->search({folder_name => $rf_name})->next();
  is ($row->outcome, 'OutcomeCompleted', 'run outcome is correct');
  is ($row->date_completed, '2025-02-28T13:30:00', 'run completion date is correct');

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
};

subtest 'load the data for a cytoprofiling run' => sub {
  plan tests => 7;

  my $rf_name = '20250602_AV244103_QC_SLIDE_2';
  my $new_dir = "$tdir/elembio/$rf_name";
  mkdir $new_dir;

  my $source = 't/data/elembio/20250602_AV244103_QC_SLIDE_2';
  cp "$source/RunParameters.json", "$new_dir/RunParameters.json";
  `cp $source/RunUploaded.json $new_dir/RunUploaded.json`;
  my $time = DateTime->new(
    year => 2025,month => 6,day => 3,hour => 3,minute => 11,second => 10
  )->epoch;
  utime $time, $time, "$new_dir/RunUploaded.json"; # set the timestamp
  my $loader = npg_warehouse::loader::elembio::run_info->new(
    schema_wh => $schema_wh, runfolder_path => $new_dir
  );
  $loader->load();

  my $row = $schema_wh->resultset('EseqRun')
    ->search({folder_name => $rf_name})->next();
  ok ($row->run_parameters, 'run params file content is loaded');
  is ($row->run_name, 'QC_SLIDE_2', 'run name is correct');
  is ($row->flowcell_id, '0124323943', 'flowcell ID is correct');
  is ($row->run_type, 'Cytoprofiling', 'cytoprofiling run type is recorded');
  is ($row->date_started, '2025-06-02T11:46:40', 'start date is correct');
  is ($row->outcome, 'OutcomeCompleted', 'run outcome is correct');
  is ($row->date_completed, '2025-06-03T03:11:10', 'run completion date is correct');
};

subtest 'do not load the data for a technical run' => sub {
  plan tests => 2;

  ############
  # Model a 'technical' run.
  # Normally only RunParameters.json is present in a run folder, no data,
  # no RunUploaded.json. FlowcellID value in RunParameters.json is unset.
  ############
  my $rf_name = '20231218_AV234003_FluidicsRun_1_A';
  my $loader = npg_warehouse::loader::elembio::run_info->new(
    schema_wh => $schema_wh,
    runfolder_path => "t/data/elembio/$rf_name"
  );
  is ($loader->load(), 0, 'loading was skipped');
  is ($schema_wh->resultset('EseqRun')->search({folder_name => $rf_name})->count(),
    0, 'a record for a technical run is not present');
};

subtest 'partial source data does not harm the warehouse record' => sub {
  plan tests => 5;

  my $rf_name = '20250815_AV244103_NT1861581K';

  # Create an empty run folder
  my $new_dir = "$tdir/elembio/$rf_name";
  mkdir $new_dir;

  cp "t/data/elembio/$rf_name/RunParameters.json", "$new_dir/RunParameters.json";
  cp "t/data/elembio/$rf_name/RunManifest.json", "$new_dir/RunManifest.json";
  cp "t/data/elembio/$rf_name/RunStats.json", "$new_dir/RunStats.json";

  my $loader = npg_warehouse::loader::elembio::run_info->new(
    schema_wh => $schema_wh,
    runfolder_path => "$new_dir"
  );
  ok($loader->load(), 'loading was performed');

  # Now check to see if the RunParameters.json data is still in the DB?
  my $pr_rs = $schema_wh->resultset('EseqRun')
    ->search({folder_name => $rf_name});

  my $db_run_info = $pr_rs->next;
  ok($db_run_info->run_manifest, "RunManifest.json data in DB");
  ok($db_run_info->run_stats, "RunStats.json data in DB");

  unlink "$new_dir/RunManifest.json";
  # Make sure any attributes of run_info are not retained between loads
  $loader = npg_warehouse::loader::elembio::run_info->new(
    schema_wh => $schema_wh,
    runfolder_path => "$new_dir"
  );
  ok($loader->load(), 'Loading ran fine again');

  my $later_run_info = $db_run_info->get_from_storage;
  ok($later_run_info->run_manifest, "RunManifest.json data still in DB");
};
