use strict;
use warnings;

use Cwd;
use File::Basename;
use File::Which;
use JSON;
use Moose::Meta::Class;
use Perl6::Slurp;
use Readonly;
use Test::Exception;
use Test::LWP::UserAgent;
use Test::More tests => 13;

use npg_testing::db;

use npg_warehouse::loader::pacbio::run;
use npg_warehouse::loader::pacbio::product;
use WTSI::NPG::HTS::PacBio::Sequel::APIClient;

Readonly::Scalar my $RUN_WELL_TABLE_NAME => q[PacBioRunWellMetric];
Readonly::Scalar my $PRODUCT_TABLE_NAME  => q[PacBioProductMetric];

isnt(which ("generate_pac_bio_id"), undef, "id generation script installed");

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

foreach my $file (glob q[t/data/pacbio/smrtlink/dataset_ccsreads/*.json]) {
  my ($name,$path,$suffix) = fileparse($file,'.json'); 
  $user_agent->map_response(
    qr{http://localhost:8071/smrt-link/datasets/ccsreads/$name},
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
  plan tests => 41;

  my $pb_api = WTSI::NPG::HTS::PacBio::Sequel::APIClient->new(user_agent => $user_agent);
  my @load_args = (dry_run       => '1',
                   pb_api_client => $pb_api,
                   mlwh_schema   => $wh_schema,
                   run_uuid      => q[288f2be0-9c7c-4930-b1ff-0ad71edae556],
                   hostname      => q[blah.sanger.ac.uk]);

  my $loader   = npg_warehouse::loader::pacbio::run->new(@load_args);
  my ($processed, $loaded, $errors) = $loader->load_run;
  
  cmp_ok($processed, '==', 1, "Dry run - Processed 1 completed run (off_instrument)");
  cmp_ok($loaded, '==', 0, "Dry run - Loaded 0 runs");
  cmp_ok($errors, '==', 0, "Dry run - Loaded 0 runs with no errors");

  my @load_args2 = (dry_run       => '0',
                    pb_api_client => $pb_api,
                    mlwh_schema   => $wh_schema,
                    run_uuid      => q[288f2be0-9c7c-4930-b1ff-0ad71edae556],
                    hostname      => q[blah.sanger.ac.uk]);

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
  is ($r2->hifi_read_quality_median, q[34], 'correct hifi read Q median for run 80685 well A1');
  is ($r2->hifi_low_quality_read_quality_median, q[16], 'correct hifi low Q read Q median for run 80685 well A1');
  is ($r2->control_num_reads, q[3914], 'correct control reads for run 80685 well A1');
  is ($r2->control_read_length_mean, q[52142], 'correct control read mean for run 80685 well A1');
  is ($r2->control_concordance_mode, q[0.89], 'correct control concordance mode for run 80685 well A1');
  is ($r2->local_base_rate, q[2.13797], 'correct local base rate for run 80685 well A1');
  is ($r2->cell_lot_number, q[416342], 'correct cell lot number for run 80685 well A1');
  is ($r2->binding_kit, q[Sequel II Binding Kit 2.0], 'correct binding kit for run 80685 well A1');
  is ($r2->sequencing_kit_lot_number, q[018942], 'correct sequencing kit lot number for run 80685 well A1');
  is ($r2->include_kinetics, q[1], 'correct include kinetics for run 80685 well A1');
  is ($r2->created_by, q[mls], 'correct created by for run 80685 well A1');
  is ($r2->sl_hostname, q[blah.sanger.ac.uk], 'correct sl hostname for run 80685 well A1');
  is ($r2->sl_run_uuid, q[288f2be0-9c7c-4930-b1ff-0ad71edae556], 'correct sl run uuid for run 80685 well A1');
  is ($r2->sl_ccs_uuid, q[74f45b56-780f-48cc-9c20-69a89ba28731], 'correct sl ccs uuid for run 80685 well A1');
  is ($r2->movie_minutes, q[1440], 'correct movie minutes for run 80685 well A1');
  is ($r2->hifi_only_reads, undef, 'correct hifi only reads for run 80685 well A1');
  is ($r2->heteroduplex_analysis, undef, 'correct heteroduplex analysis for run 80685 well A1');

  is ($r2->run_status, q[Complete], 'correct run status for run 80685 well A1');
  is ($r2->well_status, q[Complete], 'correct well status for run 80685 well A1');

  is ($r2->id_pac_bio_product, q[9c665f0ee068e3c81d7846b4a800931f8c3b11ceb5e02ccff847738b05f67bc8],
    'correct product id for run 80685 well A1 in run well metrics table');
 
  my $id  = $r2->id_pac_bio_rw_metrics_tmp;
  my $rs3 = $wh_schema->resultset($PRODUCT_TABLE_NAME)->search({id_pac_bio_rw_metrics_tmp => $id,});
  is ($rs3->count, 1, '1 loaded row found for run 80685 well A1 in pac_bio_product_metrics');
  my $r3 = $rs3->next;

  is ($r3->id_pac_bio_product, q[119d325b4c97aac11edd56f80e68818ac15627f2d6c59ffdad8dbecd5ee890cc],
    'correct product id for run 80685 well A1 tag ACACTAGATCGCGTGTT in product metrics table');

  my $rs4 = $wh_schema->resultset($RUN_WELL_TABLE_NAME)->search
    ({pac_bio_run_name => '80685', well_label => 'B1'});
  is ($rs4->count, 1, '1 loaded row found for run 80685 well B1 in pac_bio_run_well_metrics');
  my $r4 = $rs4->next;

  is ($r4->polymerase_num_reads, q[4698488], 'correct polymerase reads for run 80685 well B1 [via rescue job]');
  is ($r4->hifi_num_reads, q[2050838], 'correct hifi reads for run 80685 well B1 [via rescue job]');
  isnt($r4->id_pac_bio_product, $r3->id_pac_bio_product, 'product ids are different for different wells');
};

subtest 'load_completed_run_mixed_analysis' => sub {
  plan tests => 16;

  my $pb_api = WTSI::NPG::HTS::PacBio::Sequel::APIClient->new(user_agent => $user_agent);

  my @load_args = (dry_run       => '0',
                   pb_api_client => $pb_api,
                   mlwh_schema   => $wh_schema,
                   run_uuid      => q[89dfd7ed-c17a-452b-85b4-526d4a035d0d],
                   hostname      => q[blah.sanger.ac.uk]);

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
  is ($r2->run_complete, q[2021-01-18T23:03:43], 'correct run complete date for run 79174 well A1');
  is ($r2->run_transfer_complete, q[2021-01-19T06:17:32], 'correct run transfer complete for run 79174 well A1');

  is ($r2->sl_hostname, q[blah.sanger.ac.uk], 'correct sl hostname for run 79174 well A1');
  is ($r2->sl_run_uuid, q[89dfd7ed-c17a-452b-85b4-526d4a035d0d], 'correct sl run uuid for run 79174 well A1');
  is ($r2->sl_ccs_uuid, q[1b70315e-03de-4344-bc16-0db4683e675e], 'correct sl ccs uuid for run 79174 well A1');
  
  my $id  = $r2->id_pac_bio_rw_metrics_tmp;
  my $rs3 = $wh_schema->resultset($PRODUCT_TABLE_NAME)->search({id_pac_bio_rw_metrics_tmp => $id,});
  is ($rs3->count, 0, '0 entries (as no pac_bio_run entry) for run 79174 well A1 in pac_bio_product_metrics');
};

subtest 'load_completed_run_on_instrument_deplexing_analysis' => sub {
  plan tests => 11;

  my $pb_api = WTSI::NPG::HTS::PacBio::Sequel::APIClient->new(user_agent => $user_agent);

  my @load_args = (dry_run       => '0',
                   pb_api_client => $pb_api,
                   mlwh_schema   => $wh_schema,
                   run_uuid      => q[909d36e5-6385-4c2a-8886-72483eb6e31f],
                   hostname      => q[blah.sanger.ac.uk]);

  my $loader   = npg_warehouse::loader::pacbio::run->new(@load_args);
  my ($processed, $loaded, $errors) = $loader->load_run;
  cmp_ok($loaded, '==', 1, "Loaded 1 completed run (OnInstrument)");
  cmp_ok($errors, '==', 0, "Loaded 1 completed run (OnInstrument) with no errors");

  my $rs = $wh_schema->resultset($RUN_WELL_TABLE_NAME)->search
    ({pac_bio_run_name => 'TRACTION-RUN-142', well_label => 'A1'});
  is ($rs->count, 1, '1 loaded row found for run TR142 well A1 in pac_bio_run_well_metrics');
  my $r = $rs->next;
  is ($r->ccs_execution_mode, q[OnInstrument], 'correct process type for run TR142 well A1');

  is ($r->hifi_only_reads, 1, 'correct hifi only reads for run 80685 well A1');
  is ($r->heteroduplex_analysis, 0, 'correct heteroduplex analysis for run TR142 well A1');
  is ($r->polymerase_num_reads, q[5959113], 'correct polymerase reads for run TR142  well A1');
  is ($r->hifi_num_reads, q[2098821], 'correct hifi reads for run TR142  well A1');
  is ($r->hifi_low_quality_num_reads, undef, 'correct hifi low quality reads for run TR142  well A1'); 
  is ($r->control_num_reads, 1061, 'correct num of control reads for run TR142  well A1');
  is ($r->demultiplex_mode, q[OnInstrument], 'correct demultiplex mode for run TR142  well A1'); 
};

subtest 'load_completed_run_on_instrument_deplexing_analysis2' => sub {
  plan tests => 13;

  my $pb_api = WTSI::NPG::HTS::PacBio::Sequel::APIClient->new(user_agent => $user_agent);

  my @load_args = (dry_run       => '0',
                   pb_api_client => $pb_api,
                   mlwh_schema   => $wh_schema,
                   run_uuid      => q[75aae6f5-fdf2-4171-a189-513488dbc91f],
                   hostname      => q[blah.sanger.ac.uk]);

  my $loader   = npg_warehouse::loader::pacbio::run->new(@load_args);
  my ($processed, $loaded, $errors) = $loader->load_run;
  cmp_ok($loaded, '==', 1, "Loaded 1 completed run (OnInstrument 2)");
  cmp_ok($errors, '==', 0, "Loaded 1 completed run (OnInstrument 2) with no errors");

  my $rs = $wh_schema->resultset($RUN_WELL_TABLE_NAME)->search
    ({pac_bio_run_name => 'TRACTION-RUN-539', well_label => 'C1'});
  is ($rs->count, 1, '1 loaded row found for run TR539 well C1 in pac_bio_run_well_metrics');
  my $r = $rs->next;
  is ($r->ccs_execution_mode, q[OnInstrument], 'correct process type for run TR539 well C1');

  is ($r->hifi_only_reads, 1, 'correct hifi only reads for run TR539 well C1');
  is ($r->heteroduplex_analysis, 0, 'correct heteroduplex analysis for run TR539 well C1');
  is ($r->polymerase_num_reads, q[17973166], 'correct polymerase reads for run TR539 well C1');
  is ($r->hifi_num_reads, q[7884017], 'correct hifi reads for run TR539 well C1');
  is ($r->hifi_low_quality_num_reads, undef, 'correct hifi low quality reads for run TR539 well C1'); 
  is ($r->control_num_reads, 2802, 'correct num of control reads for run TR539 well C1');
  is ($r->demultiplex_mode, q[OnInstrument], 'correct demultiplex_mode for run TR539 well C1'); 
  is ($r->hifi_barcoded_reads, q[7861277], 'correct num of hifi barcoded reads for run TR539 well C1'); 
  is ($r->hifi_bases_in_barcoded_reads, q[93682134754],
     'correct num of hifi bases in barcoded reads for run TR539 well C1'); 
};

subtest 'load_in_progress_run' => sub {
  plan tests => 7;

  my $pb_api = WTSI::NPG::HTS::PacBio::Sequel::APIClient->new(user_agent => $user_agent);

  my @load_args = (dry_run       => '0',
                   pb_api_client => $pb_api,
                   mlwh_schema   => $wh_schema,
                   run_uuid      => q[d4c8636a-25f3-4874-b816-b690bbe31b2c],
                   hostname      => q[blah.sanger.ac.uk]);

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

subtest 'load_multiple_sample_run' => sub {
  plan tests => 7;

  my $pb_api = WTSI::NPG::HTS::PacBio::Sequel::APIClient->new(user_agent => $user_agent);

  my @load_args = (dry_run       => '0',
                   pb_api_client => $pb_api,
                   mlwh_schema => $wh_schema,
                   run_uuid => q[81913778-242d-401e-86c6-69bd4d619d8e],
                   hostname => q[blah.sanger.ac.uk]);

  my $loader = npg_warehouse::loader::pacbio::run->new(@load_args);
  my ($processed, $loaded, $errors) = $loader->load_run;

  cmp_ok ($loaded, '==', 1, "Loaded 1 multi sample run");
  cmp_ok ($errors, '==', 0, "Loaded 1 multi sample run with no errors");

  my $rw_rs = $wh_schema->resultset($RUN_WELL_TABLE_NAME)->search
    ({pac_bio_run_name => 'TRACTION-RUN-219', well_label => 'A1'});
  is ($rw_rs->count, 1, '1 row loaded for run ... well A1 in pac_bio_run_well_metrics');

  my $rw_r = $rw_rs->next;
  my $id = $rw_r->id_pac_bio_rw_metrics_tmp;
  my $p_rs = $wh_schema->resultset($PRODUCT_TABLE_NAME)->search
    ({id_pac_bio_rw_metrics_tmp => $id,});
  is ($p_rs->count, 8, '8 rows loaded for run ... well A1 in pac_bio_product_metrics');

  my $p_r1 = $p_rs->next;
  cmp_ok($rw_r->id_pac_bio_product, 'ne', $p_r1->id_pac_bio_product,
    'sample id_product is different from well id_product for multi sample run');

  my $p_r2 = $p_rs->next;
  cmp_ok($rw_r->id_pac_bio_product, 'ne', $p_r2->id_pac_bio_product,
    'sample id_product is different from well id_product for multi sample run');
  cmp_ok($p_r1->id_pac_bio_product, 'ne', $p_r2->id_pac_bio_product,
    'sample id_products are different for multi sample run');
};

subtest 'load_missing_moviename_run' => sub {
  plan tests => 6;

  my $pb_api = WTSI::NPG::HTS::PacBio::Sequel::APIClient->new(user_agent => $user_agent);

  my @load_args = (dry_run       => '0',
                   pb_api_client => $pb_api,
                   mlwh_schema => $wh_schema,
                   run_uuid => q[773e3b36-f33b-438a-bee4-fe85f07154fa],
                   hostname => q[blah.sanger.ac.uk]);

  my $loader = npg_warehouse::loader::pacbio::run->new(@load_args);
  my ($processed, $loaded, $errors) = $loader->load_run;

  cmp_ok ($loaded, '==', 1, "Loaded 1 missing moviename run");
  cmp_ok ($errors, '==', 0, "Loaded 1 missing moviename run with no errors");

  my $rw_rs = $wh_schema->resultset($RUN_WELL_TABLE_NAME)->search
    ({pac_bio_run_name => 'TRACTION-RUN-624', well_label => 'A1'});
  is ($rw_rs->count, 1, '1 row loaded for run ... well A1 in pac_bio_run_well_metrics');
  my $r = $rw_rs->next;
  is ($r->movie_name, q[m64178e_230613_122744],
      'correct movie name for TRACTION-RUN-624 well A1');

  my $rw_rs2 = $wh_schema->resultset($RUN_WELL_TABLE_NAME)->search
    ({pac_bio_run_name => 'TRACTION-RUN-624', well_label => 'B1'});
  is ($rw_rs2->count, 1, '1 row loaded for run ... well B1 in pac_bio_run_well_metrics');
  my $r2 = $rw_rs2->next;
  is ($r2->movie_name, q[m64178e_230614_172701],
      'correct movie name for TRACTION-RUN-624 well B1');

}; 

subtest 'fail_to_load_non_existent_run' => sub {
  plan tests => 2;

  my $pb_api = WTSI::NPG::HTS::PacBio::Sequel::APIClient->new(user_agent => $user_agent);

  my @load_args = (dry_run       => '0',
                   pb_api_client => $pb_api,
                   mlwh_schema   => $wh_schema,
                   run_uuid      => q[XXXXXXXX],
                   hostname      => q[blah.sanger.ac.uk]);

  my $loader   = npg_warehouse::loader::pacbio::run->new(@load_args);
  my ($processed, $loaded, $errors) = $loader->load_run;

  cmp_ok($loaded, '==', 0, "Loaded 0 runs - as run doesn't exist");
  cmp_ok($loaded, '==', 0, "Loaded 0 runs - as run doesn't exist");
};


subtest 'detect_incorrect_id_length' => sub {
  plan tests => 8;

  my $pb_api = WTSI::NPG::HTS::PacBio::Sequel::APIClient->new(user_agent => $user_agent);

  my @load_args = (dry_run       => '0',
                   pb_api_client => $pb_api,
                   mlwh_schema   => $wh_schema,
                   run_uuid      => q[d4c8636a-25f3-4874-b816-b690bbe31b2c],
                   hostname      => q[blah.sanger.ac.uk]);

  $wh_schema->resultset('PacBioProductMetric')->search()->delete();
  $wh_schema->resultset('PacBioRunWellMetric')->search()->delete();
  my $loader = npg_warehouse::loader::pacbio::run->new(@load_args);
  my ($num_processed, $num_loaded, $num_errors) = $loader->load_run();
  is($num_loaded, 1, "Loaded 1 run");             
  is($num_processed, 1, "Processed one run");
  is($num_errors, 0, "Had no errors");

  local $ENV{PATH} = getcwd()."/t/scripts:$ENV{PATH}";
  is (which ('generate_pac_bio_id'), getcwd().'/t/scripts/generate_pac_bio_id', 'Incorrect id generation script added to path');
  open my $id_product_script, q[-|], 'generate_pac_bio_id'
    or die ('Cannot generate id_product');
  my $id_product = <$id_product_script>;
  $id_product =~ s/\s//xms;
  close $id_product_script
    or die('Could not close id_product generation script');

  is ($id_product, 'notanid', 'Incorrect length id generated');
 
  $wh_schema->resultset('PacBioProductMetric')->search()->delete();
  $wh_schema->resultset('PacBioRunWellMetric')->search()->delete();
  $loader = npg_warehouse::loader::pacbio::run->new(@load_args);
  ($num_processed, $num_loaded, $num_errors) = $loader->load_run();
  is($num_loaded, 0, "Loaded 0 runs - error generating product id");             
  is($num_processed, 1, "Processed one run");
  is($num_errors, 1, "Had one error");
};

subtest 'test_linking_and_relinking' => sub {
  plan tests => 15;

  my $pb_api = WTSI::NPG::HTS::PacBio::Sequel::APIClient->new(user_agent => $user_agent);

  my @load_args = (dry_run       => 0,
                   pb_api_client => $pb_api,
                   mlwh_schema   => $wh_schema,
                   run_uuid      => q[d4c8636a-25f3-4874-b816-b690bbe31b2c],
                   hostname      => q[blah.sanger.ac.uk]);

  $wh_schema->resultset('PacBioProductMetric')->search()->delete();
  $wh_schema->resultset('PacBioRunWellMetric')->search()->delete();

  my $run_rs = $wh_schema->resultset('PacBioRun')
                         ->search({pac_bio_run_name => '80685'});
  my $pb_run_name = '80863';
  # CHANGING LIMS DATA, KEEP THIS TEST AT THE END OF THE FILE
  while (my $row = $run_rs->next()) {
    $row->update({pac_bio_run_name => $pb_run_name});
  } 

  my $loader = npg_warehouse::loader::pacbio::run->new(@load_args);
  my ($num_processed, $num_loaded, $num_errors) = $loader->load_run();
  is($num_loaded, 1, "Loaded 1 run");             
  is($num_processed, 1, "Processed one run");
  is($num_errors, 0, "Had no errors");

  my $p_rs = $wh_schema->resultset('PacBioProductMetric')->search({});
  is ($p_rs->count(), 4, '4 linking rows have been created');
  is ((grep {defined $_->id_pac_bio_tmp} $p_rs->all()), 4,
    'all rows are linked to the run table');

  my $fk_value = $wh_schema->resultset('PacBioRun')->search(
    {pac_bio_run_name => $pb_run_name, well_label => 'B1'})
    ->next->id_pac_bio_tmp;
  $wh_schema->resultset('PacBioProductMetric')
    ->search({id_pac_bio_tmp => $fk_value})->next()
    ->update({id_pac_bio_tmp => undef});
  $p_rs = $wh_schema->resultset('PacBioProductMetric')->search({});
  is ((grep {defined $_->id_pac_bio_tmp} $p_rs->all()), 3,
    '3 rows are currently linked to the run table');

  $loader = npg_warehouse::loader::pacbio::run->new(@load_args);
  ($num_processed, $num_loaded, $num_errors) = $loader->load_run(); 
  is($num_loaded, 1, "Loaded 1 run");             
  is($num_processed, 1, "Processed one run");
  is($num_errors, 0, "Had no errors");
  is ((grep {defined $_->id_pac_bio_tmp} $p_rs->all()), 4,
    '4 rows are linked to the run table');

  $wh_schema->resultset('PacBioProductMetric')->search({})  
    ->update({id_pac_bio_tmp => undef});
  is ((grep {defined $_->id_pac_bio_tmp} $p_rs->all()), 0,
    'no rows are linked to the run table');
  
  $loader = npg_warehouse::loader::pacbio::run->new(@load_args); 
  ($num_processed, $num_loaded, $num_errors) = $loader->load_run(); 
  is($num_loaded, 1, "Loaded 1 run");             
  is($num_processed, 1, "Processed one run");
  is($num_errors, 0, "Had no errors");
  is ((grep {defined $_->id_pac_bio_tmp} $p_rs->all()), 4,
    '4 rows are linked to the run table');
};

1;
