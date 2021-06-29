use strict;
use warnings;

use File::Basename;
use JSON;
use Moose::Meta::Class;
use Perl6::Slurp;
use Readonly;
use Test::Exception;
use Test::LWP::UserAgent;
use Test::More tests => 6;
use Test::Warn;


use npg_testing::db;
use t::util;

use npg_warehouse::loader::pacbio::run;
use WTSI::NPG::HTS::PacBio::Sequel::APIClient;

Readonly::Scalar my $RUN_WELL_TABLE_NAME => q[PacBioRunWellMetric];
Readonly::Scalar my $PRODUCT_TABLE_NAME  => q[PacBioProductMetric];

my $user_agent = Test::LWP::UserAgent->new(network_fallback => 1);

foreach my $file (glob q[t/data/pacbio/smrtlink/runs_collections/*.json]) {
  my ($name,$path,$suffix) = fileparse($file,'.json'); 
  $user_agent->map_response(
    qr{http://localhost:8071/smrt-link/runs/$name/collections},
    HTTP::Response->new('200', 'OK', ['Content-Type' => 'application/json'], slurp $file));
}

foreach my $file (glob q[t/data/pacbio/smrtlink/runs/*.json]) {
  my ($name,$path,$suffix) = fileparse($file,'.json');
  $user_agent->map_response(
    qr{http://localhost:8071/smrt-link/runs/$name},
    HTTP::Response->new('200', 'OK', ['Content-Type' => 'application/json'], slurp $file));
}

foreach my $file (glob q[t/data/pacbio/smrtlink/dataset_subreads_reports/*.json]) {
  my ($name,$path,$suffix) = fileparse($file,'.json'); 
  $user_agent->map_response(
    qr{http://localhost:8071/smrt-link/datasets/subreads/$name/reports},
    HTTP::Response->new('200', 'OK', ['Content-Type' => 'application/json'], slurp $file));
}

foreach my $file (glob q[t/data/pacbio/smrtlink/dataset_ccsreads_reports/*.json]) {
  my ($name,$path,$suffix) = fileparse($file,'.json');
  $user_agent->map_response(
    qr{http://localhost:8071/smrt-link/datasets/ccsreads/$name/reports},
    HTTP::Response->new('200', 'OK', ['Content-Type' => 'application/json'], slurp $file));
}

foreach my $file (glob q[t/data/pacbio/smrtlink/datasets/*.json]) {
  my ($name,$path,$suffix) = fileparse($file,'.json'); 
  $user_agent->map_response(
    qr{http://localhost:8071/smrt-link/datasets/$name},
    HTTP::Response->new('200', 'OK', ['Content-Type' => 'application/json'], slurp $file));
}

lives_ok{ $user_agent } 'web user agent handle created';


my $util = Moose::Meta::Class->create_anon_class(
  roles => [qw/npg_testing::db/])->new_object({});

my ($wh_schema)= $util->create_test_db(q[WTSI::DNAP::Warehouse::Schema],
                                       q[t/data/fixtures/wh_pacbio]);

lives_ok{ $wh_schema } 'warehouse test db created';


subtest 'load_completed_run_off_instrument_analysis' => sub {
  plan tests => 24;

  my $pb_api = WTSI::NPG::HTS::PacBio::Sequel::APIClient->new(user_agent => $user_agent);
  my @load_args = (dry_run       => '1',
                   pb_api_client => $pb_api,
                   mlwh_schema   => $wh_schema,
                   run_uuid      => q[288f2be0-9c7c-4930-b1ff-0ad71edae556]);

  my $loader   = npg_warehouse::loader::pacbio::run->new(@load_args);
  my ($processed, $loaded, $errors) = $loader->load_run;
  
  cmp_ok($processed, '==', 1, "Dry run - Processed 1 completed run (off_instrument)");
  cmp_ok($loaded, '==', 0, "Dry run - Loaded 0 runs");
  cmp_ok($errors, '==', 0, "Dry run - Loaded 0 runs with no errors");

  my @load_args2 = (dry_run       => '0',
                    pb_api_client => $pb_api,
                    mlwh_schema   => $wh_schema,
                    run_uuid      => q[288f2be0-9c7c-4930-b1ff-0ad71edae556]);

  my $loader2   = npg_warehouse::loader::pacbio::run->new(@load_args2);
  my ($processed2, $loaded2, $errors2) = $loader2->load_run;
  
  cmp_ok($processed2, '==', 1, "Processed 1 completed run (off_instrument)");
  cmp_ok($loaded2, '==', 1, "Loaded 1 completed run (off_instrument)");
  cmp_ok($errors2, '==', 0, "Loaded 1 completed run (off_instrument) with no errors");

  my $rs = $wh_schema->resultset($RUN_WELL_TABLE_NAME)->search({pac_bio_run_name => '80685',});
  is ($rs->count, 4, '4 loaded rows found for run 80685 in pac_bio_run_well_metrics');

  my $r = $rs->next;
  is ($r->instrument_name, q[64097e], 'correct instrument name for run 80685');
  is ($r->movie_name, q[m64097e_210318_144326], 'correct movie name for run 80685');

  my $rs2 = $wh_schema->resultset($RUN_WELL_TABLE_NAME)->search
    ({pac_bio_run_name => '80685', well_label => 'A1'});
  is ($rs2->count, 1, '1 loaded row found for run 80685 well A1 in pac_bio_run_well_metrics');
  my $r2 = $rs2->next;
  is ($r2->ccs_execution_mode, q[OffInstrument], 'correct process type for run 80685 well A1');
  is ($r2->polymerase_read_bases, q[414404656086], 'correct polymerase bases for run 80685 well A1');
  is ($r2->polymerase_num_reads, q[5379015], 'correct polymerase reads for run 80685 well A1');
  is ($r2->hifi_read_bases, q[24739994857], 'correct hifi bases for run 80685 well A1');
  is ($r2->hifi_num_reads, q[2449034], 'correct hifi reads for run 80685 well A1');
  is ($r2->control_num_reads, q[3914], 'correct control reads for run 80685 well A1');
  is ($r2->control_read_length_mean, q[52142], 'correct control read mean for run 80685 well A1');
  is ($r2->local_base_rate, q[2.13797], 'correct local base rate for run 80685 well A1');

  is ($r2->run_status, q[Complete], 'correct run status for run 80685 well A1');
  is ($r2->well_status, q[Complete], 'correct well status for run 80685 well A1');
 
  my $id  = $r2->id_pac_bio_rw_metrics_tmp;
  my $rs3 = $wh_schema->resultset($PRODUCT_TABLE_NAME)->search({id_pac_bio_rw_metrics_tmp => $id,});
  is ($rs3->count, 1, '1 loaded row found for run 80685 well A1 in pac_bio_product_metrics');

  my $rs4 = $wh_schema->resultset($RUN_WELL_TABLE_NAME)->search
    ({pac_bio_run_name => '80685', well_label => 'B1'});
  is ($rs4->count, 1, '1 loaded row found for run 80685 well B1 in pac_bio_run_well_metrics');
  my $r4 = $rs4->next;

  is ($r4->polymerase_num_reads, q[4698488], 'correct polymerase reads for run 80685 well B1 [via rescue job]');
  is ($r4->hifi_num_reads, q[2050838], 'correct hifi reads for run 80685 well B1 [via rescue job]');
};

subtest 'load_completed_run_mixed_analysis' => sub {
  plan tests => 11;

  my $pb_api = WTSI::NPG::HTS::PacBio::Sequel::APIClient->new(user_agent => $user_agent);

  my @load_args = (dry_run       => '0',
                   pb_api_client => $pb_api,
                   mlwh_schema   => $wh_schema,
                   run_uuid      => q[89dfd7ed-c17a-452b-85b4-526d4a035d0d]);

  my $loader   = npg_warehouse::loader::pacbio::run->new(@load_args);
  my ($processed, $loaded, $errors) = $loader->load_run;
  cmp_ok($loaded, '==', 1, "Loaded 1 completed run (mixed)");
  cmp_ok($errors, '==', 0, "Loaded 1 completed run (mixed) with no errors");

  my $rs = $wh_schema->resultset($RUN_WELL_TABLE_NAME)->search
    ({pac_bio_run_name => '79174', well_label => 'B1'});
  is ($rs->count, 1, '1 loaded row found for run 79174 well B1 in pac_bio_run_well_metrics');
  my $r = $rs->next;
  is ($r->ccs_execution_mode, q[OffInstrument], 'correct process type for run 79174 well B1');

  my $rs2 = $wh_schema->resultset($RUN_WELL_TABLE_NAME)->search
    ({pac_bio_run_name => '79174', well_label => 'A1'});
  is ($rs2->count, 1, '1 loaded row found for run 79174 well A1 in pac_bio_run_well_metrics');
  my $r2 = $rs2->next;
  is ($r2->ccs_execution_mode, q[OnInstrument], 'correct process type for run 79174 well A1');
  is ($r2->polymerase_read_bases, q[392452455322], 'correct polymerase bases for run 79174 well A1');
  is ($r2->polymerase_num_reads, q[6245434], 'correct polymerase reads for run 79174 well A1');
  is ($r2->run_status, q[Complete], 'correct run status for run 79174 well A1');
  is ($r2->well_status, q[Complete], 'correct well status for run 79174 well A1');

  my $id  = $r2->id_pac_bio_rw_metrics_tmp;
  my $rs3 = $wh_schema->resultset($PRODUCT_TABLE_NAME)->search({id_pac_bio_rw_metrics_tmp => $id,});
  is ($rs3->count, 0, '0 entries (as no pac_bio_run entry) for run 79174 well A1 in pac_bio_product_metrics');
};

subtest 'load_in_progress_run' => sub {
  plan tests => 7;

  my $pb_api = WTSI::NPG::HTS::PacBio::Sequel::APIClient->new(user_agent => $user_agent);

  my @load_args = (dry_run       => '0',
                   pb_api_client => $pb_api,
                   mlwh_schema   => $wh_schema,
                   run_uuid      => q[d4c8636a-25f3-4874-b816-b690bbe31b2c]);

  my $loader   = npg_warehouse::loader::pacbio::run->new(@load_args);
  my ($processed, $loaded, $errors) = $loader->load_run;

  cmp_ok($loaded, '==', 1, "Loaded 1 in progress run");
  cmp_ok($errors, '==', 0, "Loaded 1 in progress run");

  my $rs = $wh_schema->resultset($RUN_WELL_TABLE_NAME)->search({pac_bio_run_name => '80863',});
  is ($rs->count, 4, '4 loaded rows found for run 80863 in pac_bio_run_well_metrics');

  my $rs2 = $wh_schema->resultset($RUN_WELL_TABLE_NAME)->search
    ({pac_bio_run_name => '80863', well_label => 'A1'})->next;
  is ($rs2->run_status, q[Running], 'correct run status for run 80863 well A1');
  is ($rs2->well_status, q[Complete], 'correct well status for run 80863 well A1');

  my $rs3 = $wh_schema->resultset($RUN_WELL_TABLE_NAME)->search
    ({pac_bio_run_name => '80863', well_label => 'C1'})->next;
  is ($rs3->run_status, q[Running], 'correct run status for run 80863 well C1');
  is ($rs3->well_status, q[Acquiring], 'correct well status for run 80863 well C1');
};

subtest 'fail_to_load_non_existent_run' => sub {
  plan tests => 2;

  my $pb_api = WTSI::NPG::HTS::PacBio::Sequel::APIClient->new(user_agent => $user_agent);

  my @load_args = (dry_run       => '0',
                   pb_api_client => $pb_api,
                   mlwh_schema   => $wh_schema,
                   run_uuid      => q[XXXXXXXX]);

  my $loader   = npg_warehouse::loader::pacbio::run->new(@load_args);
  my ($processed, $loaded, $errors) = $loader->load_run;

  cmp_ok($loaded, '==', 0, "Loaded 0 runs - as run doesn't exist");
  cmp_ok($loaded, '==', 0, "Loaded 0 runs - as run doesn't exist");
};

1;
