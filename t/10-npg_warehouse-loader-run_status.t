use strict;
use warnings;
use Test::More tests => 18;
use Test::Exception;
use Moose::Meta::Class;
use npg_testing::db;

use_ok('npg_warehouse::loader::run_status');

my $util = Moose::Meta::Class->create_anon_class(
  roles => [qw/npg_testing::db/])->new_object({});

my ($schema_wh, $schema_mlwh, $schema_npg);
{
  my $schema_package = q[npg_tracking::Schema];
  my $fixtures_path = q[t/data/fixtures/npg];
  lives_ok{ $schema_npg  = $util->create_test_db($schema_package, $fixtures_path) } 'npg test db created';
  $schema_package = q[npg_warehouse::Schema];
  lives_ok{ $schema_wh  = $util->create_test_db($schema_package) } 'wh test db created';
  $schema_package = q[WTSI::DNAP::Warehouse::Schema];
  lives_ok{ $schema_mlwh  = $util->create_test_db($schema_package) } 'mlwh test db created';
}

{
  my $rsloader;
  foreach my $wh (($schema_wh, $schema_mlwh)) {
    lives_ok {$rsloader  = npg_warehouse::loader::run_status->new( 
                                             schema_npg => $schema_npg, 
                                             schema_wh  => $wh
                                           )
    } 'object instantiated by passing schema objects to the constructor';
    isa_ok ($rsloader, 'npg_warehouse::loader::run_status');

    lives_ok {$rsloader->_copy_table('RunStatusDict')} 'copy run_status_dict table';
    lives_ok {$rsloader->_copy_table('RunStatus')} 'copy run_status table';
    lives_ok {$rsloader->copy_npg_tables()} 'copy  tables';

    is ($wh->resultset($rsloader->_prefix .'RunStatusDict')->search({})->count, 24, '24 rows loaded to the dictionary');
    is ($wh->resultset($rsloader->_prefix .'RunStatus')->search({})->count, 223, '223 rows loaded to the run status table');  
  }
}

1;
