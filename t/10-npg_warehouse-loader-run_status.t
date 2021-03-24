use strict;
use warnings;
use Test::More tests => 10;
use Test::Exception;
use Moose::Meta::Class;

use npg_testing::db;

use_ok('npg_warehouse::loader::run_status');

my $util = Moose::Meta::Class->create_anon_class(
  roles => [qw/npg_testing::db/])->new_object({});

my $schema_npg;
lives_ok{ $schema_npg  = $util->create_test_db(
  q[npg_tracking::Schema], q[t/data/fixtures/npg])
} 'npg test db created';
my $wh;
lives_ok{ $wh  = $util->create_test_db(
  q[WTSI::DNAP::Warehouse::Schema]) } qq[test wh db created];

{
  my $rsloader;
  lives_ok {$rsloader  = npg_warehouse::loader::run_status->new( 
                                             schema_npg => $schema_npg, 
                                             schema_wh  => $wh
                                           )
  } 'object instantiated by passing schema objects to the constructor';
  isa_ok ($rsloader, 'npg_warehouse::loader::run_status');

  lives_ok {$rsloader->_copy_table('RunStatusDict')} 'copy run_status_dict table';
  lives_ok {$rsloader->_copy_table('RunStatus')} 'copy run_status table';
  lives_ok {$rsloader->copy_npg_tables()} 'copy  tables';
  is ($wh->resultset('IseqRunStatusDict')->search({})->count, 24,
    '24 rows loaded to the dictionary');
  is ($wh->resultset('IseqRunStatus')->search({})->count, 257,
    'all rows loaded to the run status table');
}

1;
