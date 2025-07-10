use strict;
use warnings;
use Test::More tests => 7;
use Test::Exception;
use JSON;
use Perl6::Slurp;
use List::MoreUtils qw/uniq/;

use npg_tracking::glossary::composition;
use npg_testing::db;
use t::util;

use_ok('npg_warehouse::loader::elembio::run');

my $util = Moose::Meta::Class->create_anon_class(
  roles => [qw/npg_testing::db/])->new_object({});

my $schema_wh  = $util->create_test_db(q[WTSI::DNAP::Warehouse::Schema],
  q[t/data/fixtures/wh]);
my $schema_npg  = $util->create_test_db(q[npg_tracking::Schema],
  q[t/data/fixtures/npg]);
my $schema_qc = $util->create_test_db(q[npg_qc::Schema],
  q[t/data/fixtures/npgqc]);

subtest 'Test raising errors' => sub {
  plan tests => 1;

  my $loader = npg_warehouse::loader::elembio::run->new(
    id_run => 1,
    runfolder_path => 't/data/elembio/doesnotexist',
    npg_tracking_schema => $schema_npg,
    npg_qc_schema => $schema_qc,
    mlwh_schema => $schema_wh 
  );
  throws_ok { $loader->load() }
    qr{Run folder path t/data/elembio/doesnotexist does not exist},
    'Error when run folder does not exist';
};

subtest 'load data for a two-lane run, with LIMS' => sub {
  plan tests => 581;

  my $id_run = 50517;

  my $rs = $schema_qc->resultset('MqcOutcomeEnt');
  for my $p ((1, 2)) {
    my $q = {id_run => $id_run, position => $p};
    $q->{'id_seq_composition'} =
      t::util::find_or_save_composition($schema_qc, $q);
    $q->{'id_mqc_outcome'} = 3; #'Accepted final'
    $rs->create($q);
  }
  $rs = $schema_qc->resultset('MqcLibraryOutcomeEnt');
  my $q = {id_run => $id_run, position => 1, tag_index => 5};
  $q->{'id_seq_composition'} =
    t::util::find_or_save_composition($schema_qc, $q);
  $q->{'id_mqc_outcome'} = 3; #'Accepted final'
  $rs->create($q);
  $q = {id_run => $id_run, position => 1, tag_index => 15};
  $q->{'id_seq_composition'} =
    t::util::find_or_save_composition($schema_qc, $q);
  $q->{'id_mqc_outcome'} = 4; #'Rejected final'
  $rs->create($q);

  my $loader = npg_warehouse::loader::elembio::run->new(
    id_run => $id_run,
    runfolder_path => 't/data/elembio/20250127_AV244103_NT1850075L',
    npg_tracking_schema => $schema_npg,
    npg_qc_schema => $schema_qc,
    mlwh_schema => $schema_wh 
  );
  isa_ok ($loader, 'npg_warehouse::loader::elembio::run');
  $loader->load();

  my $rl_rs = $schema_wh->resultset('EseqRunLaneMetric')
    ->search({id_run => $id_run}, {order_by => {'-asc' => 'lane'}});
  is ($rl_rs->count(), 2, 'two rows are retrieved');
  my $lane1 = $rl_rs->next();
  is ($lane1->lane, 1, 'data for lane 1 is present');
  my $lane2 = $rl_rs->next();
  is ($lane2->lane, 2, 'data for lane 2 is present');

  # Test values that are the same for both lanes.
  for my $lane (($lane1, $lane2)) {
    is ($lane->id_run, $id_run, 'run id is correct');
    is ($lane->run_folder_name, '20250127_AV244103_NT1850075L', 'run folder name is correct');
    is ($lane->run_started->datetime, '2025-01-27T15:41:07', 'run started date is correct');
    is ($lane->run_complete->datetime, '2025-01-29T02:12:27', 'run complete date is correct');
    is ($lane->cancelled, 0, 'run is not cancelled');
    is ($lane->flowcell_barcode, '2427452508', 'flowcell barcode is corect');
    is ($lane->paired_read, 1, 'the run has paired reads');
    is ($lane->cycles, 310, 'actual cycle count is correct');
    is ($lane->run_priority, 1, 'correct run priority');
    is ($lane->instrument_name, 'AV2', 'correct instrument name');
    is ($lane->instrument_external_name, 'AV244103', 'correct external instrument name');
    is ($lane->instrument_model, 'AVITI24', 'correct instrument model');
    is ($lane->instrument_side, 'A', 'correct instrument side');
    is ($lane->qc_seq, 1, 'QC outcome is correct');
  }

  is ($lane1->tags_decode_percent, 95.53, 'tags decode percent lane1');
  is ($lane1->num_polonies, 375961923, 'number of polonies lane 1');
  is ($lane2->tags_decode_percent, 95.43, 'tags decode percent lane2');
  is ($lane2->num_polonies, 388791863, 'number of polonies lane 2');

  # The same pool in both lanes

  my $num_pooled_samples = 22;
  my $pr_rs = $schema_wh->resultset('EseqProductMetric')->search({id_run => $id_run});
  is ($pr_rs->count, $num_pooled_samples*2+2, 'number of product entries');
  my @lane1_products = $pr_rs->search({lane => 1})->all();
  my $lane1_tag0 = shift @lane1_products;
  is ($lane1_tag0->tag_index, 0, 'tag zero is the first product');
  my @lane2_products = $pr_rs->search({lane => 2})->all();
  my $lane2_tag0 = shift @lane2_products;
  is ($lane2_tag0->tag_index, 0, 'tag zero is the first product');
  
  for my $tag0_row ( $lane1_tag0, $lane2_tag0 ) {
    ok (!$tag0_row->elembio_samplename, 'sample name is undefined');
    ok (!$tag0_row->elembio_project, 'project is undefined');
    ok (!$tag0_row->tag_sequence, 'index1 barcode is undefined');
    ok (!$tag0_row->tag2_sequence, 'index2 barcode is undefined');
    ok (!defined $tag0_row->id_eseq_flowcell_tmp, 'tag0 row is not linked to LIMS');
  }
  is ($lane1_tag0->tag_decode_count, 16802647, 'read count is correct');
  is (sprintf('%0.2f', $lane1_tag0->tag_decode_percent), '4.47',
    'percent of reads is correct');
  is ($lane2_tag0->tag_decode_count, 17753846, 'read count is correct');
  is (sprintf('%0.2f', $lane2_tag0->tag_decode_percent), '4.57',
    'percent of reads is correct');

  my $expected = from_json(slurp
    't/data/elembio/20250127_AV244103_NT1850075L/20250127_AV244103_NT1850075L/expected_sample_stats.json'
  );
  my @fks = map { int } qw/289 290 292 293 277 287 279 280 296
                           288 291 299 298 294 284 300 295 286/;
  for my $lane ((1, 2)) {
    my @products = $lane == 1 ? @lane1_products : @lane2_products;
    my $num_polonies_lane = $lane == 1 ? 375961923 : 388791863;
    for  my $p (@products) {
      my $ti = $p->tag_index;
      is ($p->elembio_samplename, $expected->{$lane}->{$ti}->[0]->{"SampleName"},
        "sample name for tag $ti lane $lane is correct");
      is ($p->tag_decode_count, $expected->{$lane}->{$ti}->[0]->{"NumPolonies"},
        "number of polonies for tag $ti lane $lane is correct");
      is (sprintf('%0.4f', $p->tag_decode_percent),
          sprintf('%0.4f', ($p->tag_decode_count/$num_polonies_lane)*100),
          'tag_decode percent is correct');

      my $expected_sequence = $expected->{$lane}->{$ti}->[0]->{"ExpectedSequence"};
      is ($p->tag_sequence, substr($expected_sequence, 0, 10),
        "index1 barcode for tag $ti lane $lane is correct");
      is ($p->tag2_sequence, substr($expected_sequence, 10),
        "index2 barcode for tag $ti lane $lane is correct");
     
      my $composition_json = qq({"components":[{"id_run":$id_run,"position":$lane,"tag_index":$ti}]});
      is ($p->eseq_composition_tmp, $composition_json,
        "composition string for tag $ti lane $lane is correct");
      my $composition = npg_tracking::glossary::composition->thaw($composition_json,
        'component_class' => 'npg_tracking::glossary::composition::component::illumina');
      is ($p->id_eseq_product, $composition->digest, 'product ID is corrcet');

      my $flag = (($ti <= 4) && ($ti >= 1)) ? 1 : 0;
      is ($p->is_sequencing_control, $flag,
        "sequencing control flag $flag is set correctly for tag $ti lane $lane");
      if ($flag) {
        ok (!defined $p->id_eseq_flowcell_tmp, 'Adept control row is not linked to LIMS');
      } else {
        my $id = $fks[$ti-5];
        if ($lane == 2) {
          $id += 100;
        }
        is ($p->id_eseq_flowcell_tmp, $id, "plex $ti lane $lane row is correctly linked to LIMS data");
      }

      is ($p->qc_seq, 1, 'QC seq is 1');
      if ( ($lane == 1) && (($ti == 5) || ($ti == 15)) ) {
        if ($ti == 5) {
          is ($p->qc_lib, 1, 'QC lib is 1');
          is ($p->qc, 1, 'QC overall is 1');
        } else {
          is ($p->qc_lib, 0, 'QC lib is 0');
          is ($p->qc, 0, 'QC overall is 0');
        }
      } else {
        ok (!defined $p->qc_lib, 'QC lib is undefined');
        is ($p->qc, 1, 'QC overall is 1');
      }
    }
  }
};

subtest 'load data for early finished/cancelled run' => sub {
  plan tests => 47;

  my $id_run = 50545;
  my $loader = npg_warehouse::loader::elembio::run->new(
    id_run => $id_run,
    runfolder_path => 't/data/elembio',
    npg_tracking_schema => $schema_npg,
    npg_qc_schema => $schema_qc,
    mlwh_schema => $schema_wh 
  );
  $loader->load();

  my $rl_rs = $schema_wh->resultset('EseqRunLaneMetric')
    ->search({id_run => $id_run}, {order_by => {'-asc' => 'lane'}});
  is ($rl_rs->count(), 2, 'two rows are retrieved');
 
  # Test values that are the same for both lanes.
  my $lane_number = 0;
  for my $lane (($rl_rs->next(), $rl_rs->next())) {
    $lane_number++;
    is ($lane->id_run, $id_run, 'run id is correct');
    is ($lane->lane, $lane_number, "lane number is $lane_number");
    is ($lane->run_folder_name, '20250513_AV244103_NT1854819U_Run2', 'run folder name is correct');
    is ($lane->run_started->datetime, '2025-05-13T14:57:41', 'run started date is correct');
    ok (!defined($lane->run_complete), 'run complete date is not set');
    is ($lane->cancelled, 0, 'run is not cancelled');
    is ($lane->flowcell_barcode, '2443597240', 'flowcell barcode is corect');
    is ($lane->paired_read, 1, 'the run has paired reads');
    is ($lane->cycles, 0, 'actual cycle count is zero');
    is ($lane->run_priority, 1, 'correct run priority');
    is ($lane->instrument_name, 'AV2', 'correct instrument name');
    is ($lane->instrument_external_name, 'AV244103', 'correct external instrument name');
    is ($lane->instrument_model, 'AVITI24', 'correct instrument model');
    is ($lane->instrument_side, 'A', 'correct instrument side');
    ok (!defined $lane->qc_seq, 'QC outcome for a lane is not defined');
    ok (!$lane->tags_decode_percent, 'tags decode percent is not defined');
    ok (!$lane->num_polonies, 'number of polonies is not defined');
  }

  my $p_rs = $schema_wh->resultset('EseqProductMetric')->search({id_run => $id_run});
  is ($p_rs->count(), 0, 'no product data');

  $schema_npg->resultset('RunStatus')->search({id_run => $id_run, iscurrent => 1})
    ->update({iscurrent => 0});
  $schema_npg->resultset('RunStatus')->create({
    date => '2025-05-14 19:08:27',
    id_run => $id_run,
    id_run_status_dict => 5,
    id_user => 7,
    iscurrent => 1
  });
  is ($schema_npg->resultset('Run')->find($id_run)->current_run_status_description(),
    'run cancelled', q(run status has been switched to 'run cancelled'));

  $loader->load();
  $rl_rs = $schema_wh->resultset('EseqRunLaneMetric')
    ->search({id_run => $id_run}, {order_by => {'-asc' => 'lane'}});
  is ($rl_rs->count(), 2, 'two rows are retrieved');
  my $lane = $rl_rs->next();
  is($lane->cancelled, 1, 'run is cancelled');
  ok (!defined($lane->run_complete), 'run complete date is not set');
  ok (!$lane->tags_decode_percent, 'tags decode percent is not defined');
  ok (!$lane->num_polonies, 'number of polonies is not defined');

  $lane = $rl_rs->next();
  is($lane->cancelled, 1, 'run is cancelled');
  ok (!defined($lane->run_complete), 'run complete date is not set');
  ok (!$lane->tags_decode_percent, 'tags decode percent is not defined');
  ok (!$lane->num_polonies, 'number of polonies is not defined');

  $p_rs = $schema_wh->resultset('EseqProductMetric')->search({id_run => $id_run});
  is ($p_rs->count(), 0, 'no product data');
};

subtest 'load data for a one-lane run, no LIMS' => sub {
  plan tests => 24;

  my $id_run = 50490;

  my $rs = $schema_qc->resultset('MqcOutcomeEnt');
  my $q = {id_run => $id_run, position => 1};
  $q->{'id_seq_composition'} =
      t::util::find_or_save_composition($schema_qc, $q);
  $q->{'id_mqc_outcome'} = 4; #'Rejected final'
  $rs->create($q);

  my $loader = npg_warehouse::loader::elembio::run->new(
    id_run => $id_run,
    runfolder_path => 't/data/elembio/20240416_AV234003_16AprilSGEB2_2x300_NT1799722A',
    npg_tracking_schema => $schema_npg,
    npg_qc_schema => $schema_qc,
    mlwh_schema => $schema_wh 
  );
  $loader->load();

  my $rl_rs = $schema_wh->resultset('EseqRunLaneMetric')
    ->search({id_run => $id_run});
  is ($rl_rs->count(), 1, 'one row is loaded');
  my $lane = $rl_rs->next();
  is ($lane->lane, 1, 'data for lane 1 is present');
  is ($lane->id_run, $id_run, 'run id is correct');
  is ($lane->run_folder_name, '20240416_AV234003_16AprilSGEB2_2x300_NT1799722A',
    'run folder name is correct');
  is ($lane->run_started->datetime, '2024-04-16T10:24:16', 'run started date is correct');
  is ($lane->run_complete->datetime, '2024-04-18T21:06:50', 'run complete date is correct');
  is ($lane->cancelled, 0, 'run is not cancelled');
  is ($lane->flowcell_barcode, '2335443584', 'flowcell barcode is corect');
  is ($lane->paired_read, 1, 'the run has paired reads');
  is ($lane->cycles, 618, 'actual cycle count is correct');
  is ($lane->run_priority, 3, 'correct run priority');
  is ($lane->instrument_name, 'AV1', 'correct instrument name');
  is ($lane->instrument_external_name, 'AV234003', 'correct external instrument name');
  is ($lane->instrument_model, 'AVITI23', 'correct instrument model');
  is ($lane->instrument_side, 'B', 'correct instrument side');
  is ($lane->qc_seq, 0, 'QC outcome is correct');
  is (sprintf('%0.2f', $lane->tags_decode_percent), '99.30', 'tags decode percent is correct');
  is ($lane->num_polonies, 363641937, 'number of polonies is correct');

  my @pr_rows = $schema_wh->resultset('EseqProductMetric')
    ->search({id_run => $id_run})->all();
  is (scalar @pr_rows, 95, '95 product rows are created'); # 90 samples, 4 controls, 1 tag zero
  is (scalar(grep { $_->lane == 1 } @pr_rows), 95, 'All rows belong to lane 1');
  is (scalar(grep { $_->id_eseq_flowcell_tmp } @pr_rows), 0,
    'no rows are linked to LIMS data');
  is (scalar(grep { defined $_->qc && ($_->qc==0) } @pr_rows), 95, 'Overall QC is 0 for all');
  is (scalar(grep { defined $_->qc_seq && ($_->qc_seq==0) } @pr_rows), 95, 'QC seq is 0 for all');
  is (scalar(grep { !defined $_->qc_lib } @pr_rows), 95, 'QC lib is undefined for all');
};

subtest 'load data for a one-lane run, Sample_barcodes 1:N, no LIMS' => sub {
  plan tests => 23;

  my $id_run = 50556;
  my $loader = npg_warehouse::loader::elembio::run->new(
    id_run => $id_run,
    runfolder_path => 't/data/elembio/20250401_AV244103_NT1853579T',
    npg_tracking_schema => $schema_npg,
    npg_qc_schema => $schema_qc,
    mlwh_schema => $schema_wh 
  );
  $loader->load();

  my $rl_rs = $schema_wh->resultset('EseqRunLaneMetric')
    ->search({id_run => $id_run});
  is ($rl_rs->count(), 1, 'one run-lane row is loaded');

  my $pr_rs = $schema_wh->resultset('EseqProductMetric')
    ->search({id_run => $id_run});
  # 4 control samples
  # 2 samples with one pair of barcodes
  # 6 samples with 144 pairs of barcodes each
  # tag zero
  is ($pr_rs->count(), 871, '871 product rows are created');
  my $samples = {};
  for ($pr_rs->all()) {
    my $name = $_->elembio_samplename;
    $name ||= 'tagzero';
    my $tag = $_->tag_sequence ? join(q[],$_->tag_sequence,$_->tag2_sequence) : 'none';
    push @{$samples->{$name}->{$_->tag_index}}, $tag;
  }
  is (scalar(keys %{$samples}), 13, '13 samples are recorded');
  ok (((exists $samples->{'tagzero'}->{0}) && (scalar @{$samples->{'tagzero'}->{0}} == 1)),
    'one row for tag zero');
  ok (((exists $samples->{'Inzolia_a'}->{11}) && (scalar @{$samples->{'Inzolia_a'}->{11}} == 1)),
    'one row for Inzolia_a');
  ok (((exists $samples->{'Inzolia_b'}->{12}) && (scalar @{$samples->{'Inzolia_b'}->{12}} == 1)),
    'one row for Inzolia_b');
  my $name2index = {'64_1_2_single_ok' => 10,
                    '64_1_1_double_good' => 5,
                    '64_1_1_single_good' => 6,
                    '64_1_2_double_contaminated' => 7,
                    '64_1_2_double_good' => 8,
                    '64_1_2_single_good' => 9};
  for my $name (keys %{$name2index}) {
    my $ti = $name2index->{$name};
    ok (((exists $samples->{$name}->{$ti}) && (scalar @{$samples->{$name}->{$ti}} == 144)),
      "144 rows for $name");
    is (scalar(uniq @{$samples->{$name}->{$ti}}), 144, "144 unique tag pair for $name");
  }
  my $sample_name = 'Inzolia_a';
  my $row = $schema_wh->resultset('EseqProductMetric')
    ->search({id_run => $id_run, elembio_samplename => $sample_name})->next();
  is ($row->tag_decode_count, 6880504, "tag decode count is correct for $sample_name");
  is ($row->tag_sequence, 'CAAGGATCGA', "first index is correct for $sample_name");
  is ($row->tag2_sequence, 'TTGTGTCTGC', "second index is correct for $sample_name");
  $sample_name = '64_1_1_double_good';
  $row = $schema_wh->resultset('EseqProductMetric')->search({
    id_run => $id_run,
    elembio_samplename => $sample_name,
    tag_sequence =>  'ACAACAGGCT',
    tag2_sequence => 'ACATTACTCG'
  })->next();
  is ($row->tag_index, 5, 'tag index is correct');
  is ($row->tag_decode_count, 214527, "tag decode count is correct for $sample_name");
};

subtest 'load data for a two-lane run, one index read, no LIMS' => sub {
  plan tests => 5;

  my $id_run = 50550;
  my $loader = npg_warehouse::loader::elembio::run->new(
    id_run => $id_run,
    runfolder_path => 't/data/elembio/20250225_AV244103_NT1850075L_NT1850808B_repeat3',
    npg_tracking_schema => $schema_npg,
    npg_qc_schema => $schema_qc,
    mlwh_schema => $schema_wh 
  );
  $loader->load();

  my $rl_rs = $schema_wh->resultset('EseqRunLaneMetric')
    ->search({id_run => $id_run});
  is ($rl_rs->count(), 2, 'two run-lane rows are loaded');
  my $pr_rs = $schema_wh->resultset('EseqProductMetric')
    ->search({id_run => $id_run});
  is ($pr_rs->count(), 3082, 'two product rows are loaded');
  is ($pr_rs->search({tag2_sequence => {'!=' => undef}})->count(), 0,
    'second tag sequence is not defined for any of the products');
  is ($pr_rs->search({tag_sequence => undef})->count(), 2,
    'first tag sequence is undefined for two products (tag zero)');
  is ($pr_rs->search({is_sequencing_control => 1})->count(), 8,
    '8 controls are present');
};

1;
