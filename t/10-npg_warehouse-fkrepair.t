use strict;
use warnings;
use Test::More tests => 10;
use Moose::Meta::Class;
use npg_testing::db;

use npg_qc::autoqc::qc_store;

my $RUN_LANE_TABLE_NAME      = q[IseqRunLaneMetric];
my $PRODUCT_TABLE_NAME       = q[IseqProductMetric];
my $LIMS_FK_COLUMN_NAME      = q[id_iseq_flowcell_tmp];

use_ok ('npg_warehouse::fk_repair');
isa_ok (npg_warehouse::fk_repair->new(), 'npg_warehouse::fk_repair');

{
  my $r = npg_warehouse::fk_repair->new();
  ok(!$r->loop, 'loop is false by default');
  ok(!$r->verbose, 'verbose is false by default');
  ok(!$r->explain, 'explain is false by default');
  is($r->sleep_time, 0, 'sleep time is zero by default');

  $r = npg_warehouse::fk_repair->new(loop => 1);
  ok($r->loop, 'loop is set to true');
  is($r->sleep_time, 300, 'sleep time is set to 300');

  $r = npg_warehouse::fk_repair->new(loop => 1, sleep_time => 120);
  ok($r->loop, 'loop is set to true');
  is($r->sleep_time, 120, 'sleep time is set to 120');
}

my $util = Moose::Meta::Class->create_anon_class(
  roles => [qw/npg_testing::db/])->new_object({});

my ($schema_npg, $schema_qc, $schema_wh);

# lives_ok{ $schema_wh  = $util->create_test_db(q[WTSI::DNAP::Warehouse::Schema],
#   q[t/data/fixtures/wh]) } 'warehouse test db created';
# lives_ok{ $schema_npg  = $util->create_test_db(q[npg_tracking::Schema],
#   q[t/data/fixtures/npg]) } 'npg test db created';
# lives_ok{ $schema_qc  = $util->create_test_db(q[npg_qc::Schema],
#   q[t/data/fixtures/npgqc]) } 'npgqc test db created';

my $init = {
             schema_npg   => $schema_npg, 
             schema_qc    => $schema_qc, 
             schema_wh    => $schema_wh,
             verbose       => 0,
             explain       => 0,
           };






1;
