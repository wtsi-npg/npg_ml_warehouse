use strict;
use warnings;
use Test::More tests => 5;
use Test::Exception;
use Moose::Meta::Class;
use npg_testing::db;

use_ok('npg_warehouse::loader::illumina::qc');

my $util = Moose::Meta::Class->create_anon_class(
  roles => [qw/npg_testing::db/])->new_object({});

my $schema_qc = $util->create_test_db(
  q[npg_qc::Schema], q[t/data/fixtures/npgqc]);

{
  my $q;
  lives_ok {
    $q = npg_warehouse::loader::illumina::qc->new(schema_qc => $schema_qc)
  } 'object instantiated by passing schema objects to the constructor';
  isa_ok ($q, 'npg_warehouse::loader::illumina::qc');

  throws_ok {$q->retrieve_cluster_density()} qr/Run id argument should be set/,
    'error if id_run arg not set';

  my $expected = {
    1 => {'raw_cluster_density' => 95465.880,  'pf_cluster_density' => 11496.220,},
    2 => {'raw_cluster_density' => 325143.800, 'pf_cluster_density' => 82325.490,},
    3 => {'raw_cluster_density' => 335626.700, 'pf_cluster_density' => 171361.900,},
    4 => {'raw_cluster_density' => 175608.400, 'pf_cluster_density' => 161077.600,},
    5 => {'raw_cluster_density' => 443386.900, 'pf_cluster_density' => 380473.100,},
    6 => {'raw_cluster_density' => 454826.200, 'pf_cluster_density' => 397424.100,},
    7 => {'raw_cluster_density' => 611192.000, 'pf_cluster_density' => 465809.300,},
    8 => {'raw_cluster_density' => 511924.700, 'pf_cluster_density' => 377133.300,},
                 };

  is_deeply ($q->retrieve_cluster_density(4333), $expected,
    'cluster densities for run 4333');
}

1;
