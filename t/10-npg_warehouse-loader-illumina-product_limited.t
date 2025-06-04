use strict;
use warnings;
use Test::More tests => 7;
use Test::Exception;
use Moose::Meta::Class;
use File::Temp qw/tempdir/;
use File::Slurp;
use File::Copy::Recursive qw/dircopy/;

use npg_tracking::glossary::composition::factory::rpt_list;
use t::util;

use_ok('npg_warehouse::loader::illumina::product_limited');

my $dir = tempdir(CLEANUP => 1);

my $util = Moose::Meta::Class->create_anon_class(
  roles => [qw/npg_testing::db/])->new_object({});

my ($schema_wh, $schema_qc);
lives_ok{ $schema_wh  = $util->create_test_db(q[WTSI::DNAP::Warehouse::Schema]) }
  'warehouse test db created';
lives_ok{ $schema_qc  = $util->create_test_db(q[npg_qc::Schema],
  q[t/data/fixtures/npgqc]) } 'npgqc test db created';

subtest 'object construction, presence of attributes' => sub {
  plan tests => 9;

  throws_ok { npg_warehouse::loader::illumina::product_limited->new(schema_qc => $schema_qc,
                                                          schema_wh => $schema_wh)
  } qr/Either rpt_list or composition_path or autoqc_path should be set/,
    'error if neither of composition-defining attributes is set';
  throws_ok { npg_warehouse::loader::illumina::product_limited->new(schema_qc => $schema_qc,
                                                          schema_wh => $schema_wh,
                                                          autoqc_path => ['t'],
                                                          composition_path => ['t'])
  } qr/Only one of rpt_list, composition_path, autoqc_path can be set/,
    'error if two composition-defining attributes are set';
  throws_ok { npg_warehouse::loader::illumina::product_limited->new(schema_qc => $schema_qc,
                                                          schema_wh => $schema_wh,
                                                          autoqc_path => ['t'],
                                                          rpt_list => ['234:1:1'])
  } qr/Only one of rpt_list, composition_path, autoqc_path can be set/,
    'error if two composition-defining attributes are set';

  my $l = npg_warehouse::loader::illumina::product_limited->new(
    schema_qc => $schema_qc, schema_wh => $schema_wh, autoqc_path => ['t']);
  isa_ok ($l, 'npg_warehouse::loader::illumina::product_limited');
  ok (!$l->can('schema_npg'), 'schema_npg accessor is not available');
  ok (!$l->can('explain'), 'explain accessor is not available');
  ok (!$l->can('get_lims_fk'), 'get_lims_fk method is not available');
  ok ($l->can('lims_fk_repair'), 'lims_fk_repair accessor is available');
  ok (!$l->lims_fk_repair, 'lims_fk_repair value is false');
};

subtest 'autoqc results from path' => sub {
  plan tests => 3;

  my $a = 't/data/runfolders/with_merges/Data/Intensities/' .
          'BAM_basecalls_20180805-013153/no_cal/archive';
  my $temp_a = join q[/], $dir, 'BAM_basecalls_20180805-013153';
  mkdir $temp_a;
  dircopy($a, $temp_a) or die "Failed to copy $a";  
  my $interop = join q[/], $temp_a, 'lane1/qc/26291_1.interop.json';
  -e $interop or die 'Copying went wrong';
  unlink $interop; 

  my $l_merged = npg_warehouse::loader::illumina::product_limited->new(
    schema_qc => $schema_qc,
    schema_wh => $schema_wh,
    autoqc_path => ["$temp_a/plex1/qc"]
  );  
  throws_ok { $l_merged->load() } qr/Failed to find the component product row/,
    'merged results cannot be loaded without rows for components being present';

  my $l = npg_warehouse::loader::illumina::product_limited->new(
    schema_qc => $schema_qc,
    schema_wh => $schema_wh,
    autoqc_path => [$temp_a, "$temp_a/lane1/qc"]
  );
  is ($l->load(), 28, '28 rows loaded');
  lives_and { is $l_merged->load(), 1 } 'merged result loaded';
};

subtest 'autoqc results for rpt_list strings' => sub {
  plan tests => 4;

  my @rpt_lists = map { '6998:1:' . $_}
                  (13 .. 18);
  push @rpt_lists, '6998:3:153';

  my $l = npg_warehouse::loader::illumina::product_limited->new(
    schema_qc => $schema_qc,
    schema_wh => $schema_wh,
    rpt_list  => \@rpt_lists
  );
  is ($l->load(), 7, '7 rows loaded');

  push @rpt_lists, '16998:3:153'; # no data
  $l = npg_warehouse::loader::illumina::product_limited->new(
    schema_qc => $schema_qc,
    schema_wh => $schema_wh,
    rpt_list  => \@rpt_lists
  );
  is ($l->load(), 7, '7 rows loaded');

  $l = npg_warehouse::loader::illumina::product_limited->new(
    schema_qc => $schema_qc,
    schema_wh => $schema_wh,
    rpt_list  => ['16998:3:153']
  );
  is ($l->load(), 0, 'no data loaded');

  $l = npg_warehouse::loader::illumina::product_limited->new(
    schema_qc => $schema_qc,
    schema_wh => $schema_wh,
    rpt_list  => ['6998:3']
  );
  is ($l->load(), 3, '3 rows loaded from tag metrics result');
};

subtest 'autoqc results for compositions from a path' => sub {
  plan tests => 1;

  my $class = 'npg_tracking::glossary::composition::factory::rpt_list';
  my @jsons = map { $_->freeze(with_class_names => 1) }
              map { $_->create_composition() }
              map { $class->new(rpt_list => $_) }
              map { '6998:1:' . $_}
              (13 .. 18);
  my $i = 0;
  foreach my $j (@jsons) {
    write_file($dir . q[/] . $i++ . '.collection.json', $j);
  }

  my $other = "$dir/other";
  mkdir $other or die 'Failed to create a directory';
  rename "$dir/1.collection.json", "$other/1.collection.json" or die 'Failed to move a file';

  my $l = npg_warehouse::loader::illumina::product_limited->new(
    schema_qc => $schema_qc,
    schema_wh => $schema_wh,
    composition_path  => [$dir, $other]
  );
  is ($l->load(), 6, '6 rows loaded');  
};

1;
