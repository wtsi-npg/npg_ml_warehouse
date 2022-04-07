#!/usr/bin/perl
use strict;
use warnings;
use Test::More tests => 11;
use Test::Exception;
use Test::Deep;

use npg_testing::db;
use npg_warehouse::loader::irods_location;

my $json_dir = 't/data/irods_locations/';
my $new_only_json = $json_dir . "new_only.json";
my $irods_locations_table_name = q[SeqProductIrodsLocation];
my $new  = [
  {
    "id_product" => "03kcie64a98b1dad5ec481b82e2465kf5a8587c537f99381093c93cd6f4kd73j",
    "seq_platform_name" => "illumina",
    "pipeline_name" => "npg-prod",
    "irods_root_collection" => "/seq/illumina/20/20202/lane2/plex3/",
    "irods_data_relative_path" => "20202_2#3.bam",
  },
  {
    "id_product" => "45fire64a98b1dad5ec36fh42e2465kf5a858736hdu99381093c93cd6f4473jdu7",
    "seq_platform_name" => "illumina",
    "pipeline_name" => "cellranger",
    "irods_root_collection" => "/seq/illumina/cellranger/path/to/coll/",
    "irods_data_relative_path" => "consensus.bam",
  },
  {
    "id_product" => "03kcie64a98b1dad5ec481b82e2465kf5a8587c537f99381093c93cd6f4kd73j",
    "seq_platform_name" => "illumina",
    "pipeline_name" => "npg-prod-alt-process",
    "irods_root_collection" => "/seq/illumina/20/20202/lane2/plex9/",
    "irods_data_relative_path" => "20202_2#9.bam",
  },
  {
    "id_product" => "m64230e_210622_180142.ccs.bc1022_BAK8B_OA--bc1022_BAK8B_OA",
    "seq_platform_name" => "pacbio",
    "pipeline_name" => "npg-prod",
    "irods_root_collection" => "/seq/pacbio/r64230e_20210618_162634/4_D01/",
    "irods_data_relative_path" => "demultiplex.bc1022_BAK8B_OA--bc1022_BAK8B_OA.bam",
  }
  # TODO: add an ONT example once id_product format is decided (here and in file)
];
my $update =
  {
    "id_product"=> "8671acb3a98b1dad5ec481b82e20e11e5a8587c537f99381093c93cd6fd3251f",
    "irods_root_collection" => "/seq/4486/",
  };

my $new_with_update =
  { # New
    "id_product"=>  "273jd788b1dad5ec483kd84h565kf5a8587c537f99381093c93cd6irov849",
    "seq_platform_name"=> "illumina",
    "pipeline_name"=>  "npg-prod",
    "irods_root_collection"=> "/seq/illumina/20/20202/lane2/plex4/",
    "irods_data_relative_path"=> "20202_2#4.bam",
  };

my $new_same_id =
  {
    "id_product" => "8671acb3a98b1dad5ec481b82e20e11e5a8587c537f99381093c93cd6fd3251f",
    "seq_platform_name" => "illumina",
    "pipeline_name" => "cellranger",
    "irods_root_collection" => "/seq/illumina/cellranger/path/to/coll/",
    "irods_data_relative_path" => "consensus.bam",
  };

my $new_same_coll =
  {
    "id_product"=> "38456dueyfjeksd5ec481b82e20e115834kdi3437f9938101029dke73c3251r",
    "seq_platform_name"=> "illumina",
    "pipeline_name"=> "npg-prod-alt-process",
    "irods_root_collection"=> "/seq/4486",
    "irods_data_relative_path"=> "4486_5.bam",
  };

my $new_same_row =
  {
    "id_product"=>  "47dje744a98b1dad5ec481293kdue5kf5a8587c537f9931029ke3cd6f419283j",
    "seq_platform_name"=> "illumina",
    "pipeline_name"=>  "npg-prod-alt-process",
    "irods_root_collection"=> "/seq/illumina/20/20202/lane6/plex1/",
    "irods_data_relative_path"=> "20202_6#1.bam",
  };

my $directory = [
  {
    "id_product" => "42kcie64a98b1dad5ec481b82e2465kf5a9647c537f99381093c93cd6fkd73rt",
    "seq_platform_name" => "illumina",
    "pipeline_name" => "npg-prod",
    "irods_root_collection" => "/seq/illumina/20/20202/lane1/plex3/",
    "irods_data_relative_path" => "20202_1#3.bam",
  },
  {
    "id_product" => "id837e64a98b1d0g8ec481b82e2465kf5a9647c537f99381093c37ddfkfuv623",
    "seq_platform_name" => "illumina",
    "pipeline_name" => "npg-prod",
    "irods_root_collection" => "/seq/illumina/20/20202/lane1/plex1/",
    "irods_data_relative_path" => "20202_1#1.bam",
  }
];


sub make_irods_location{
  my ($schema_wh, $json) = @_;
  return npg_warehouse::loader::irods_location->new(
      schema_wh => $schema_wh,
      path => $json,
    );
}


sub get_product_row {
  my ($irods_locations, $product) = @_;
  return $irods_locations->schema_wh->resultset($irods_locations_table_name)->
      find($product, {key => 'pi_root_product'});
}

sub get_columns_in_files {
  my $row = shift;
  my %columns = $row->get_columns;
  my %columns_in_files = map { $_ => $columns{$_} } qw/id_product seq_platform_name pipeline_name irods_root_collection irods_data_relative_path/;
  return \%columns_in_files;
}

my $util = Moose::Meta::Class->create_anon_class(
  roles => [qw/npg_testing::db/])->new_object({});

my $schema_wh;
lives_ok { $schema_wh  = $util->create_test_db(
  q[WTSI::DNAP::Warehouse::Schema], q[t/data/fixtures/wh_npg])
} 'warehouse test db created';

subtest 'object creation' => sub {
  plan tests => 8;
  my $irods_location;
  lives_ok {
    $irods_location = make_irods_location($schema_wh, $new_only_json)
  } 'object instantiated by passing schema object to the constructor';
  isa_ok($irods_location, 'npg_warehouse::loader::irods_location');
  is($irods_location->path, $new_only_json, 'json file set correctly');
  is($irods_location->dry_run, 0, 'not a dry run by default');
  foreach my $product (@{$new}) {
    my $row = get_product_row($irods_location, $product);
    is($row, undef, 'new product not present in initial db');
  }
};

subtest 'dry-run' => sub {
  plan tests => 8;
  my $irods_location;
  lives_ok {$irods_location = npg_warehouse::loader::irods_location->new(
    schema_wh => $schema_wh,
    path      => $new_only_json,
    dry_run   => 1,
    verbose   => 1,
  );
  } 'object instantiated with optional attributes';
  is($irods_location->verbose, 1, 'loader is verbose');
  my @subhashes = ();
  foreach my $product (@{$new}) {
    push @subhashes, subhashof($product);
  }
  cmp_deeply($irods_location->get_product_locations, bag(@subhashes),
  'products read in correctly');
  lives_ok { $irods_location->load }
    'product loading function succeeds';
  foreach my $product (@{$new}) {
    my $row = get_product_row($irods_location, $product);
    is($row, undef, 'dry-run has not added the product to the table');
  }
};

subtest 'load new products' => sub {
  plan tests => 13;
  my $irods_location = make_irods_location($schema_wh, $new_only_json);
  foreach my $product(@{$new}){
    my $row = get_product_row($irods_location, $product);
    is ($row, undef, 'new product not in table before loading');
  }
  lives_ok { $irods_location->load }
    'product loading function succeeds';
  foreach my $product (@{$new}) {
    my $row = get_product_row($irods_location, $product);
    is($row->in_storage, 1, 'new product loaded');
    cmp_deeply(get_columns_in_files($row), $product, 'correct values for new product');
  }
};

subtest 'load updated product' => sub {
  plan tests => 5;
  my $irods_location = make_irods_location($schema_wh,
  $json_dir . 'update_only.json');
  my $initial_row = get_product_row($irods_location, $update);
  is($initial_row->in_storage, 1, 'row in table before update');
  is($initial_row->get_column('pipeline_name'), 'npg-prod',
  'row has correct value before update');
  lives_ok { $irods_location->load }
    'product loading function succeeds';
  my $updated_row = get_product_row($irods_location, $update);
  is($updated_row->in_storage, 1, 'row in table after update');
  is($updated_row->get_column('pipeline_name'), 'npg-prod-alt-process',
  'row has new value after update');
};

subtest 'load mixed new and updated products' => sub {
  plan tests => 8;
  my $irods_location = make_irods_location($schema_wh,
  $json_dir . 'new_and_update.json');
  my $initial_row = get_product_row($irods_location, $update);
  is($initial_row->get_column('pipeline_name'),
  'npg-prod-alt-process',
  'updated row has correct pipeline name value before update');
  is($initial_row->get_column('irods_data_relative_path'), '4486_1.bam',
  'updated row has correct relative path value before update');
  my $new_row = get_product_row($irods_location, $new_with_update);
  is($new_row, undef, 'new product not in table before loading');
  lives_ok{ $irods_location->load}
    'product loading function succeeds';
  $new_row = get_product_row($irods_location, $new_with_update);
  is($new_row->in_storage, 1, 'new product loaded');
  cmp_deeply(get_columns_in_files($new_row), $new_with_update,
  'correct values for new product');
  my $updated_row = get_product_row($irods_location, $update);
  is($updated_row->get_column('pipeline_name'), 'npg-prod',
  'updated row has new pipeline name value after update');
  is($updated_row->get_column('irods_data_relative_path'), '4486_2.bam',
  'updated row has new relative path value after update');

};

subtest 'load new row with the same product id as a present row' => sub {
  plan tests => 5;
  my $irods_location = make_irods_location($schema_wh,
  $json_dir . 'new_same_id.json');
  my $present_row_before = get_product_row($irods_location, $update);
  my $new_row = get_product_row($irods_location, $new_same_id);
  is($new_row, undef, 'new row not in table before loading');
  lives_ok{ $irods_location->load}
    'product loading function succeeds';
  my $present_row_after = get_product_row($irods_location, $update);
  cmp_deeply({$present_row_before->get_columns}, {$present_row_after->get_columns},
  'previously present row did not change');
  $new_row = get_product_row($irods_location, $new_same_id);
  is($new_row->in_storage, 1, 'new product loaded');
  cmp_deeply(get_columns_in_files($new_row), $new_same_id,
  'correct values for new product');
};

subtest 'load new row with the same root collection as a present row' => sub {
  plan tests => 5;
  my $irods_location = make_irods_location($schema_wh,
  $json_dir . 'new_same_coll.json');
  my $present_row_before = get_product_row($irods_location, $update);
  my $new_row = get_product_row($irods_location, $new_same_coll);
  is($new_row, undef, 'new row not in table before loading');
  lives_ok{ $irods_location->load}
    'product loading function succeeds';
  my $present_row_after = get_product_row($irods_location, $update);
  cmp_deeply({$present_row_before->get_columns}, {$present_row_after->get_columns},
  'previously present row did not change');
  $new_row = get_product_row($irods_location, $new_same_coll);
  is($new_row->in_storage, 1, 'new product loaded');
  cmp_deeply(get_columns_in_files($new_row), $new_same_coll,
  'correct values for new product');
};

subtest 'load new row twice from same file' => sub {
  plan tests => 3;
  my $irods_location = make_irods_location($schema_wh,
  $json_dir . 'new_same_row.json');
  my $new_row = get_product_row($irods_location, $new_same_row);
  is($new_row, undef, 'new row not in table before loading');
  lives_ok{ $irods_location->load}
    'product loading function succeeds';
  $new_row = get_product_row($irods_location, $new_same_row);
  cmp_deeply(get_columns_in_files($new_row), $new_same_row,
  'latest values for new product kept');
};

subtest 'update row twice from same file' => sub {
  plan tests => 3;
  my $irods_location = make_irods_location($schema_wh,
  $json_dir . 'update_same_row.json');
  lives_ok{ $irods_location->load}
    'product loading function succeeds';
  my $row = get_product_row($irods_location, $update);
  is($row->get_column('irods_data_relative_path'), '4486_20.bam',
  'updated row has correct relative path');
  is($row->get_column('pipeline_name'), 'npg-kept',
  'updated row has correct pipeline name (updated twice)');
};

subtest 'load rows from a directory' => sub {
  plan tests => 5;
  my $irods_location = make_irods_location($schema_wh,
  $json_dir . 'dir');
  lives_ok{ $irods_location->load}
      'product loading function succeeds';
  foreach my $product (@{$directory}) {
    my $row = get_product_row($irods_location, $product);
    is($row->in_storage, 1, 'new product loaded');
    cmp_deeply(get_columns_in_files($row), $product, 'correct values for new product');
  }
};

1;

