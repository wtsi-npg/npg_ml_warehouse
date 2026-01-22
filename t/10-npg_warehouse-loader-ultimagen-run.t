use strict;
use warnings;
use Test::More tests => 10;
use Test::Exception;
use Archive::Extract;
use File::Temp qw/ tempdir /;

use npg_qc::autoqc::db_loader;
use npg_testing::db;
use t::util;

use_ok('npg_warehouse::loader::ultimagen::run');

my $util = Moose::Meta::Class->create_anon_class(
  roles => [qw/npg_testing::db/])->new_object({});
my $schema_wh  = $util->create_test_db(q[WTSI::DNAP::Warehouse::Schema],
  q[t/data/fixtures/wh]);
my $schema_npg  = $util->create_test_db(q[npg_tracking::Schema],
  q[t/data/fixtures/npg]);
my $schema_qc = $util->create_test_db(q[npg_qc::Schema],
  q[t/data/fixtures/npgqc]);

######
# All tests are for run 51579, which matches LIMS record with id_wafer_lims
# 131_NT114848K_2. Initial fixtures - tracking record and useq_wafer record.
#

my $tmp = tempdir(CLEANUP => 1);
my $id_run = 51579;
my $num_samples = 96;
my $num_all_samples = $num_samples + 2; # tag zero and control data
my $rf_path = 't/data/ultimagen/430136-20251213_0356';
my $m_path = 't/data/ultimagen/430136-20251213_0356/manifest.csv';
my $library_pool = $schema_npg->resultset('Run')->find($id_run)->batch_id;
my %db_init = (schema_wh  => $schema_wh,
               schema_npg => $schema_npg,
               schema_qc  => $schema_qc);

is ($schema_wh->resultset('UseqRunMetric')->search({})->count(), 0,
  'run table is empty');
is ($schema_wh->resultset('UseqProductMetric')->search({})->count(), 0,
  'product table is empty');

subtest 'Object construction' => sub {
  plan tests => 5;

  throws_ok { npg_warehouse::loader::ultimagen::run->new(%db_init, runfolder_path => 't') }
    qr/Attribute \(id_run\) is required/, 'error if id_run attribute is not set';
  throws_ok { npg_warehouse::loader::ultimagen::run->new(%db_init, id_run => $id_run) }
    qr/Either runfolder_path or manifest_path attribute should be set/,
    'error if neither runfolder_path nor manifest_path is given';
  lives_ok { npg_warehouse::loader::ultimagen::run->new(
    %db_init, runfolder_path => 't', id_run => $id_run) }
    'no error if both id_run and runfolder_path are given';
  lives_ok { npg_warehouse::loader::ultimagen::run->new(
    %db_init, manifest_path => $m_path, id_run => $id_run) }
    'no error if both id_run and runfolder_path are given';
  lives_ok { npg_warehouse::loader::ultimagen::run->new(
    %db_init, manifest_path => $m_path, id_run => $id_run, runfolder_path => 't') }
    'no error if id_run runfolder_path and manifest_path are given';
};

subtest 'Load with no autoqc data' => sub {
  plan tests => 4;

  lives_ok { npg_warehouse::loader::ultimagen::run->new(
    %db_init, runfolder_path => $rf_path, id_run => $id_run)->load() }
    'loading without autoqc data is OK';
  is ($schema_wh->resultset('UseqRunMetric')->search({})->count(), 1,
    'one row in the run table');
  my $rs = $schema_wh->resultset('UseqProductMetric')->search({});
  is ($rs->count(), $num_samples, "$num_samples rows in the product table");
  is (scalar (grep { $_->id_useq_wafer_tmp } $rs->all()), $num_samples,
    "$num_samples rows are linked to useq_wafer table");
};

subtest 'Load with autoqc data' => sub {
  plan tests => 57;

  my $ae = Archive::Extract->new(
    archive => 't/data/ultimagen/430136-20251213_0356/51579_autoqc_results.tar.gz'
  );
  $ae->extract(to => $tmp) or die $ae->error;

  npg_qc::autoqc::db_loader->new(
    path    =>["$tmp/51579_autoqc_results"],
    schema  => $schema_qc,
  )->load();

  lives_ok { npg_warehouse::loader::ultimagen::run->new(
    %db_init, runfolder_path => $rf_path, id_run => $id_run)->load() }
    'loading with autoqc data is OK';
  is ($schema_wh->resultset('UseqRunMetric')->search({})->count(), 1,
    'one row in the run table');
  my $rs = $schema_wh->resultset('UseqProductMetric')->search({});
  is ($rs->count(), $num_all_samples, "$num_all_samples rows in the product table");
  is (scalar (grep { $_->id_useq_wafer_tmp } $rs->all()), $num_samples,
    $num_samples . ' rows are linked to useq_wafer table');

  my $run_row = $schema_wh->resultset('UseqRunMetric')->find($id_run);
  is ($run_row->ultimagen_run_id, '430136', 'Ultimagen Run ID');
  is ($run_row->ultimagen_library_pool, '131_NT114848K_2',
    'Ultimagen Library_Pool value');
  is ($run_row->instrument_name, 'UG1', 'instrument name');
  is ($run_row->instrument_external_name, 'V125', 'instrument external name');
  is ($run_row->instrument_model, 'UG 100', 'instrument format name');
  is ($run_row->run_folder_name, '430136-20251213_0356', 'run folder name');
  is ($run_row->run_priority,3, 'run priority');
  is ($run_row->cancelled, 0, 'run is not cancelled');
  is ($run_row->run_in_progress, '2025-12-13T03:56:00',
    '"run in progress" time stamp');
  is ($run_row->run_archived, undef, '"run archived" time stamp is undefined');
  is ($run_row->qc_seq, undef, 'qc_seq is undefined');
  is ($run_row->num_reads, 12117765442, 'number of reads');
  is ($run_row->input_num_reads, 16356365456, 'number of input reads');
  is ($run_row->tags_decode_percent, 96.7570069260629, 'tag decode percent');

  my $row = $schema_wh->resultset('UseqProductMetric')
    ->search({id_run => $id_run, tag_index => 4})->next();
  is ($row->id_useq_product,
    '00e23960e8c6b308dfbfc8859b600ec94567abd7f171f0ec916b886025b2ee63',
    'product id');
  is ($row->is_sequencing_control, 0, 'not a sequencing control');
  is ($row->ultimagen_index_label, 'Z0004', 'index label');
  is ($row->ultimagen_index_sequence, 'CTGTGTAGGCATGAT', 'index sequence');
  is ($row->ultimagen_sample_id, '100', 'sample name');
  is ($row->ultimagen_library_name, '13STDY243406', 'library name');
  is ($row->ultimagen_application_type, 'native', 'application  type');
  is ($row->qc_seq, undef, 'qc_seq is undefined');
  is ($row->qc_lib, undef, 'qc_lib is undefined');
  is ($row->qc, undef, 'qc is undefined');
  is ($row->tag_decode_count, 98838658, 'sample tag decode count');
  is ($row->tag_decode_percent, 0.815650859666145, 'sample tag decode percent');
  is ($row->q20_yield_kb, 22625290, 'q20 yield');
  is ($row->q30_yield_kb, 18120436, 'q30 yield');
  is ($row->total_yield_kb, 27302148, 'sample total yield');
  
  $row = $schema_wh->resultset('UseqProductMetric')
    ->search({id_run => $id_run, tag_index => 0})->next();
  is ($row->id_useq_product,
    'd298205a9533bd593358cfc858e85408c5f9fdc0a2c646e3af783d7376cdb0e0',
    'product id');
  is ($row->is_sequencing_control, 0, 'not a sequencing control');
  is ($row->ultimagen_index_label, undef, 'index label in undefined');
  is ($row->ultimagen_index_sequence, undef, 'no index sequence');
  is ($row->ultimagen_sample_id, undef, 'sample id is not defined');
  is ($row->ultimagen_library_name, undef, 'library name is not defined');
  is ($row->ultimagen_application_type, undef, 'application  type is not defined');
  is ($row->tag_decode_count, 392978294, 'tag zero tag decode count');
  is ($row->tag_decode_percent, 3.24299307393707, 'tag zero tag decode percent');
  is ($row->q20_yield_kb, undef, 'q20 yield undefined');
  is ($row->q30_yield_kb, undef, 'q30 yield undefined');
  is ($row->total_yield_kb, undef, 'total yield undefined');
  
  $row = $schema_wh->resultset('UseqProductMetric')
    ->search({id_run => $id_run, tag_index => 9999})->next();
  is ($row->id_useq_product,
    'fb1e997154f73147c4154a42925d3357e6ee630103e3572f5b458a5016d985fb',
    'product id');
  is ($row->is_sequencing_control, 1, 'is a sequencing control');
  is ($row->ultimagen_index_label, undef, 'index label in undefined');
  is ($row->ultimagen_index_sequence, 'TT', 'correct index sequence');
  is ($row->ultimagen_sample_id, undef, 'sample id is not defined');
  is ($row->ultimagen_library_name, undef, 'library name is not defined');
  is ($row->ultimagen_application_type, undef, 'application  type is not defined');
  is ($row->tag_decode_count, 257587809, 'control tag decode count');
  is ($row->tag_decode_percent, 2.12570387034564, 'control tag decode percent');
  is ($row->q20_yield_kb, 66843726, 'control q20 yield');
  is ($row->q30_yield_kb, 57064907, 'control q30 yield');
  is ($row->total_yield_kb, 76937990, 'control total yield');
};

subtest 'No linking without library pool value' => sub {
  plan tests => 4;

  $schema_wh->resultset('UseqProductMetric')->search({})->delete();
  $schema_wh->resultset('UseqRunMetric')->search({})->delete();
  $schema_npg->resultset('Run')->find($id_run)->update({batch_id => undef});

  lives_ok { npg_warehouse::loader::ultimagen::run->new(
    %db_init, runfolder_path => $rf_path, id_run => $id_run)->load() }
    'loading is OK';
  is ($schema_wh->resultset('UseqRunMetric')->find($id_run)->ultimagen_library_pool,
    undef, 'ultimagen_library_pool value is unset');
  my $rs = $schema_wh->resultset('UseqProductMetric')->search({});
  is ($rs->count(), $num_all_samples, "$num_all_samples rows in the product table");
  is (scalar (grep { $_->id_useq_wafer_tmp } $rs->all()), 0,
    'no rows are linked to useq_wafer table');
};

subtest 'No linking with wrong library pool value' => sub {
  plan tests => 4;

  $schema_npg->resultset('Run')->find($id_run)->update({batch_id => 'invalid'});

  lives_ok { npg_warehouse::loader::ultimagen::run->new(
    %db_init, runfolder_path => $rf_path, id_run => $id_run)->load() }
    'loading is OK';
  is ($schema_wh->resultset('UseqRunMetric')->find($id_run)->ultimagen_library_pool,
    'invalid', 'ultimagen_library_pool value is set');
  my $rs = $schema_wh->resultset('UseqProductMetric')->search({});
  is ($rs->count(), $num_all_samples, "$num_all_samples rows in the product table");
  is (scalar (grep { $_->id_useq_wafer_tmp } $rs->all()), 0,
    'no rows are linked to useq_wafer table');
};

subtest 'Partial linking' => sub {
  plan tests => 4;

  $schema_npg->resultset('Run')->find($id_run)->update({batch_id => $library_pool});
  my @sample_rows = $schema_wh->resultset('UseqWafer')->search({})->all;
  my $count = 0;
  for my $row (@sample_rows) {
    if ($count < 10) {
      $row->delete();
    } else {
      last;
    }
    $count++;
  }

  lives_ok { npg_warehouse::loader::ultimagen::run->new(
    %db_init, runfolder_path => $rf_path, id_run => $id_run)->load() }
    'loading is OK';
  is ($schema_wh->resultset('UseqRunMetric')->find($id_run)->ultimagen_library_pool,
    $library_pool, 'ultimagen_library_pool value is set');
  my $rs = $schema_wh->resultset('UseqProductMetric')->search({});
  is ($rs->count(), $num_all_samples, "$num_all_samples rows in the product table");
  my $num_linked = $num_samples-10;
  is (scalar (grep { $_->id_useq_wafer_tmp } $rs->all()), $num_linked,
    "$num_linked rows are linked to useq_wafer table");
};

subtest 'Loading mqc outcomes' => sub {
  plan tests => 25;

  my $rs = $schema_qc->resultset('MqcOutcomeEnt');
  my $q = {id_run => $id_run, position => 1};
  $q->{'id_seq_composition'} =
    t::util::find_or_save_composition($schema_qc, $q);
  $q->{'id_mqc_outcome'} = 3; #'Accepted final'
  $rs->create($q);
  
  $rs = $schema_qc->resultset('MqcLibraryOutcomeEnt');
  my $num_fails = 3;
  for my $tag_index ((1 .. 96)) {
    my $q = {id_run => $id_run, position => 1, tag_index => $tag_index};
    $q->{'id_seq_composition'} =
      t::util::find_or_save_composition($schema_qc, $q);
    if ($tag_index <= $num_fails) {
      $q->{'id_mqc_outcome'} = 4; #'Rejected final'
    } else {
      $q->{'id_mqc_outcome'} = 3; #'Accepted final'
    }
    $rs->create($q);
  }

  lives_ok { npg_warehouse::loader::ultimagen::run->new(
    %db_init, runfolder_path => $rf_path, id_run => $id_run)->load() }
    'loading is OK';

  my $row = $schema_wh->resultset('UseqRunMetric')->find($id_run);
  is ($row->qc_seq, 1, 'qc_seq value is set to 1 for the run');

  my @rows = $schema_wh->resultset('UseqProductMetric')->search({})->all;
  is (scalar @rows, $num_all_samples, "$num_all_samples product rows");
  
  my $num_assessed = $num_samples + 1; # plus the control
  my $num_passes = $num_samples - $num_fails;
  my $num_passed_qseq = $num_passes + 1; # plus the control

  is (scalar (grep { $_->qc_seq == 1 } grep { defined $_->qc_seq } @rows),
    $num_assessed, "$num_assessed products have qc_seq value set to 1");

  is (scalar (grep { defined $_->qc_lib } @rows), $num_samples,
    "$num_samples products have qc_lib value set");
  is (scalar (grep { $_->qc_lib == 0 } grep { defined $_->qc_lib } @rows),
    $num_fails, "$num_fails products have qc_lib value set to 0");
  is (scalar (grep { $_->qc_lib == 1 } grep { defined $_->qc_lib } @rows),
    $num_passes, "$num_passes products have qc_lib value set to 1");
  
  is (scalar (grep { defined $_->qc } @rows), $num_assessed,
    "$num_assessed products have overall qc value set");
  is (scalar (grep { $_->qc == 0 } grep { defined $_->qc } @rows),
    $num_fails, "$num_fails products have overall qc value set to 0");
  is (scalar (grep { $_->qc == 1 } grep { defined $_->qc } @rows),
    $num_passed_qseq, "$num_passed_qseq products have overall qc value set to 1");

  $row = $schema_wh->resultset('UseqProductMetric')
    ->search({tag_index => 0})->next;
  is ($row->qc_lib, undef, 'qc_lib is undefined for tag zero');
  is ($row->qc_seq, undef, 'qc_seq is undefined for tag zero');
  is ($row->qc, undef, 'qc is undefined for tag zero');
  
  $row = $schema_wh->resultset('UseqProductMetric')
    ->search({is_sequencing_control => 1})->next;
  is ($row->qc_lib, undef, 'qc_lib is undefined for the control');
  is ($row->qc_seq, 1, 'qc_seq is 1 for the control');
  is ($row->qc, 1, 'qc is 1 for the control');

  my @failed_rows = grep { ($_->tag_index > 0) &&  ($_->tag_index < 4) } @rows;
  for my $row (@failed_rows) {
    is ($row->qc_lib, 0, 'qc_lib is 0 for a failed sample');
    is ($row->qc_seq, 1, 'qc_seq is 1 for a failed sample');
    is ($row->qc, 0, 'qc is 0 for a failed sample');
  }
};

1;
