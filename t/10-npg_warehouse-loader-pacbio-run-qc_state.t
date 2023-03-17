use strict;
use warnings;
use Moose::Meta::Class;
use Test::LWP::UserAgent;
use YAML qw/LoadFile/;
use JSON;
use Perl6::Slurp;
use List::MoreUtils qw/uniq/;
use Test::More tests => 10;
use Test::Exception;

use_ok('npg_warehouse::loader::pacbio::qc_state');

my $util = Moose::Meta::Class->create_anon_class(
  roles => [qw/npg_testing::db/])->new_object({});
my $fixtures_dir = q[t/data/fixtures/wh_pacbio2];
my $wh_schema = $util->create_test_db(
  q[WTSI::DNAP::Warehouse::Schema], $fixtures_dir);

my $wells = LoadFile(join q[/], $fixtures_dir, q[200-PacBioRunWellMetric.yml]); 

my $server_url = q[https://langqc.com];
my $request_url = join q[/], $server_url, qw/api products qc/; 

my $user_agent = Test::LWP::UserAgent->new(network_fallback => 0);

my $ref_333 = {
  run_name    => 'TRACTION-RUN-333',           
  server_url  => $server_url,
  useragent   => $user_agent,
  mlwh_schema => $wh_schema,
};

$user_agent->map_response(
  qr/api\/products\/qc/,
  HTTP::Response->new(
    '200', 'OK', ['Content-Type' => 'application/json'], encode_json({})
  )
);

subtest 'unknown run' => sub {
  plan tests => 4;

  my $qc_loader = npg_warehouse::loader::pacbio::qc_state->new(
    run_name    => 'UNKNOWN',
    server_url  => $server_url,
    useragent   => $user_agent,
    mlwh_schema => $wh_schema,
    dry_run     => 0,
  );
  isa_ok($qc_loader, 'npg_warehouse::loader::pacbio::qc_state');
  is_deeply ($qc_loader->product_ids, [], 'an empy list of product IDs');
  is_deeply ($qc_loader->_qc_states, {}, 'an empty hash of QC states');
  lives_ok { $qc_loader->load_qc_state() } 'no data is not a problem';
};

subtest 'run with no QC states' => sub {
  plan tests => 3;

  my $qc_loader = npg_warehouse::loader::pacbio::qc_state->new($ref_333);
  my %product_ids = map { $_ => 1 } @{$qc_loader->product_ids};
  my %expected_product_ids =
    map  { $_->{id_pac_bio_product} => 1 }
    grep { $_->{pac_bio_run_name} eq 'TRACTION-RUN-333' } @{$wells};

  is_deeply (\%product_ids, \%expected_product_ids,
    'correct list of product IDs');
  is_deeply ($qc_loader->_qc_states, {}, 'an empty hash of QC states');
  lives_ok { $qc_loader->load_qc_state() } 'no QC states is not a problem'; 
};

subtest 'process entities that are not in mlwh' => sub {                                        
  plan tests => 2;
  
  $user_agent->unmap_all();
  $user_agent->map_response(
    qr/api\/products\/qc/,
    HTTP::Response->new(
      '200', 'OK', ['Content-Type' => 'application/json'],
      encode_json({foo => "moo"})
    )
  );

  my $qc_loader = npg_warehouse::loader::pacbio::qc_state->new($ref_333);
  is_deeply ($qc_loader->_qc_states, {foo => "moo"}, 'response as expected');
  lives_ok { $qc_loader->load_qc_state() }
    'QC state for an unexpected product id is not a problem';
};

subtest 'error conditions not leading to a failure' => sub {                                        
  plan tests => 3;

  # Valid product ID.
  my $id = q[5a0d0383da70b962afca0e75761fa494f902efc0de5b1df270dc2e3f5d24baad];
  
  # Duplicate entry for the seq QC state.
  my $response = {
    $id => [
      {"qc_state" => "Passed",
       "is_preliminary" => 0,
       "qc_type" => "sequencing",
       "outcome" => 1,
       "id_product" => $id,
       "date_updated" => "2022-11-11T00:00:00"},
      {"qc_state" => "Passed",
       "is_preliminary" => 0,
       "qc_type" => "library",
       "outcome" => 1,
       "id_product" => $id,
       "date_updated" => "2022-11-11T00:00:00"},
      {"qc_state" => "Passed",
       "is_preliminary" => 0,
       "qc_type" => "sequencing",
       "outcome" => 1,
       "id_product" => $id,
       "date_updated" => "2022-11-11T00:00:00"}
   ]
  };

  $user_agent->unmap_all();
  $user_agent->map_response(
    qr/api\/products\/qc/,
    HTTP::Response->new(
      '200', 'OK', ['Content-Type' => 'application/json'],
      encode_json($response)
    )
  );
  my $qc_loader = npg_warehouse::loader::pacbio::qc_state->new($ref_333);
  lives_ok { $qc_loader->load_qc_state() } 'duplicate seq. QC state - no error';

  # Remove the duplicate seq. entry, create an undefined entry.
  pop @{$response->{$id}};
  $response->{$id}->[0]->{qc_state} = undef;
  $user_agent->unmap_all();
  $user_agent->map_response(
    qr/api\/products\/qc/,
    HTTP::Response->new(
      '200', 'OK', ['Content-Type' => 'application/json'],
      encode_json($response)
    )
  );
  $qc_loader = npg_warehouse::loader::pacbio::qc_state->new($ref_333);
  lives_ok { $qc_loader->load_qc_state() }
   'undefined values in seq. QC state - no error';

  # Remove the last seq. entry.
  shift @{$response->{$id}};
  $user_agent->unmap_all();
  $user_agent->map_response(
    qr/api\/products\/qc/,
    HTTP::Response->new(
      '200', 'OK', ['Content-Type' => 'application/json'],
      encode_json($response)
    )
  );
  $qc_loader = npg_warehouse::loader::pacbio::qc_state->new($ref_333);
  lives_ok { $qc_loader->load_qc_state() } 'no seq. QC state - no error';
};

subtest 'deal with error response' => sub {
  plan tests => 1;

  $user_agent->unmap_all();
  $user_agent->map_response(
    qr/api\/products\/qc/,
    HTTP::Response->new(
      '500', 'SERVER ERROR', ['Content-Type' => 'application/json'],
      encode_json(["foo1", "foo2"])
    )
  );
  my $qc_loader = npg_warehouse::loader::pacbio::qc_state->new($ref_333);
  lives_ok { $qc_loader->load_qc_state() } 'no error';
};

subtest 'load qc state first time' => sub {                                        
  plan tests => 35;

  my $json_333 = slurp('t/data/pacbio/langqc/TRACTION-RUN-333.json');

  $user_agent->unmap_all();
  $user_agent->map_response(
    qr/api\/products\/qc/,
    HTTP::Response->new(
      '200', 'OK', ['Content-Type' => 'application/json'], $json_333
    )
  );
  my $rs_333 = $wh_schema->resultset('PacBioRunWellMetric')
               ->search({pac_bio_run_name => 'TRACTION-RUN-333'});

  $ref_333->{dry_run} = 1;  
  my $qc_loader = npg_warehouse::loader::pacbio::qc_state->new($ref_333);
  is (keys %{$qc_loader->_qc_states}, 6, 'retrieved QC states for six wells');
  lives_ok { $qc_loader->load_qc_state() } 'load QC states for 6 wells';
  my $num_updated_rows =
    $rs_333->search({'qc_seq_state' => {'!=', undef}})->count();
  is ($num_updated_rows, 0, 'dry run - no rows are updated');

  $ref_333->{dry_run} = 0;
  # Reset product QC outcome so that we can detect the update.
  $rs_333->search_related_rs('pac_bio_product_metrics', {})
         ->update({'qc' => 2});
  $qc_loader = npg_warehouse::loader::pacbio::qc_state->new($ref_333);
  lives_ok { $qc_loader->load_qc_state() } 'load QC states for 6 wells';
  $num_updated_rows =
    $rs_333->search({'qc_seq_state' => {'!=', undef}})->count();
  is ($num_updated_rows, 6, 'live run - no rows are updated');

  my $input = decode_json($json_333);
  foreach my $product_id (keys %{$input}) {
    my $row = $rs_333->find({id_pac_bio_product => $product_id});
    _test_row_update($input->{$product_id}->[0], $row);
  }
};

subtest 'missing product rows' => sub {
  plan tests => 1;
  
  # Delete product rows.
  $wh_schema->resultset('PacBioRunWellMetric')
    ->search({pac_bio_run_name => 'TRACTION-RUN-333'})
    ->search_related_rs('pac_bio_product_metrics', {})
    ->delete();

  my $qc_loader = npg_warehouse::loader::pacbio::qc_state->new($ref_333);
  lives_ok { $qc_loader->load_qc_state() } 'no error loading data';
};

subtest 'update QC states' => sub {
  plan tests => 63;

  my $json_351 = slurp('t/data/pacbio/langqc/TRACTION-RUN-351.json');
  my $run_name = 'TRACTION-RUN-351';

  $user_agent->unmap_all();
  $user_agent->map_response(
    qr/api\/products\/qc/,
    HTTP::Response->new(
      '200', 'OK', ['Content-Type' => 'application/json'], $json_351
    )
  );
  my $rs_351 = $wh_schema->resultset('PacBioRunWellMetric')
               ->search({pac_bio_run_name => $run_name});
  my $ref_351 = {
    run_name    => $run_name,           
    server_url  => $server_url,
    useragent   => $user_agent,
    mlwh_schema => $wh_schema,
  };

  my $qc_loader = npg_warehouse::loader::pacbio::qc_state->new($ref_351);
  is (keys %{$qc_loader->_qc_states}, 4, 'retrieved QC states for four wells');
  lives_ok { $qc_loader->load_qc_state() } 'loaded QC states for four wells';
  my $num_updated_rows =
    $rs_351->search({'qc_seq_state' => {'!=', undef}})->count();
  is ($num_updated_rows, 4, 'four rows are updated');

  my $input = decode_json($json_351);
  foreach my $product_id (keys %{$input}) {
    my $row = $rs_351->find({id_pac_bio_product => $product_id});
    _test_row_update($input->{$product_id}->[0], $row);
  }

  my @products = $rs_351->search({'well_label' => 'A1'})
                 ->search_related_rs('pac_bio_product_metrics', {})->all();
  is (@products, 2, 'two product rows for well A1');
  my @qc = uniq map { $_->qc } @products; 
  is (@qc, 1, 'one unique value for QC results');
  is ($qc[0], 1, 'pass is recorded');

  @products = $rs_351->search({'well_label' => 'B1'})
              ->search_related_rs('pac_bio_product_metrics', {})->all();
  is (@products, 1, 'one product row for well B1');
  is ($products[0]->qc, 0, 'fail is recorded');

  @products = $rs_351->search({'well_label' => ['C1', 'D1']})
              ->search_related_rs('pac_bio_product_metrics', {})->all();
  is (@products, 4, 'four product rows for wells C1 and D1');
  @qc = uniq grep { defined $_ } map { $_->qc } @products; 
  is (@qc, 0, 'no values are recorded for prelim. QC results');

  # Update two prelim. results (wells C1 and D1)
  my $d =
  $input->{'4c8631d2bd634bcc6ab8ebcf60f1c26b2fd32fc1c9ee5ac3dec43d76e47ca2d6'}->[0];
  $d->{'is_preliminary'} = \0; # converts to correct false JSON value
  $d->{'date_updated'} = '2022-11-28T09:00:00';
  $d =
  $input->{'b0f4d9c7b1d9db86965655cc58052f8943152324f72143dd7e81f1590b911b49'}->[0];
  $d->{'is_preliminary'} = \0;
  $d->{'qc_state'} = 'Passed';
  $d->{'outcome'} = \1; # converts to correct true JSON value
  $d->{'date_updated'} = '2022-11-28T10:00:00';
  
  $user_agent->unmap_all();
  $user_agent->map_response(
    qr/api\/products\/qc/,
    HTTP::Response->new(
      '200', 'OK', ['Content-Type' => 'application/json'], encode_json($input)
    )
  );

  $qc_loader = npg_warehouse::loader::pacbio::qc_state->new($ref_351);
  lives_ok { $qc_loader->load_qc_state() } 'loaded QC states for four wells';

  for (qw/4c8631d2bd634bcc6ab8ebcf60f1c26b2fd32fc1c9ee5ac3dec43d76e47ca2d6
          b0f4d9c7b1d9db86965655cc58052f8943152324f72143dd7e81f1590b911b49/) {
    $input->{$_}->[0]->{'is_preliminary'} = 0;
  }

  foreach my $product_id (keys %{$input}) {
    my $row = $rs_351->find({id_pac_bio_product => $product_id});
    _test_row_update($input->{$product_id}->[0], $row);
  }
 
  @products = $rs_351->search({'well_label' => 'C1'})
              ->search_related_rs('pac_bio_product_metrics', {})->all();
  is (@products, 2, 'two product rows for well C1'); 
  is ($products[0]->qc, 0, 'fail is recorded');
  is ($products[1]->qc, 0, 'fail is recorded');

  @products = $rs_351->search({'well_label' => 'D1'})
              ->search_related_rs('pac_bio_product_metrics', {})->all();
  is (@products, 2, 'two product rows for well D1');
  @qc = uniq map { $_->qc } @products; 
  is (@qc, 1, 'one unique value for QC results');
  is ($qc[0], 1, 'pass is recorded');
};

subtest 'QC states for a well with many products' => sub {
  plan tests => 65;

  my $json_92 = slurp('t/data/pacbio/langqc/TRACTION-RUN-92.json');
  my $run_name = 'TRACTION-RUN-92';

  $user_agent->unmap_all();
  $user_agent->map_response(
    qr/api\/products\/qc/,
    HTTP::Response->new(
      '200', 'OK', ['Content-Type' => 'application/json'], $json_92
    )
  );
  my $rs_92 = $wh_schema->resultset('PacBioRunWellMetric')
               ->search({pac_bio_run_name => $run_name});
  my $ref_92 = {
    run_name    => $run_name,           
    server_url  => $server_url,
    useragent   => $user_agent,
    mlwh_schema => $wh_schema,
  };

  my $qc_loader = npg_warehouse::loader::pacbio::qc_state->new($ref_92);
  is (keys %{$qc_loader->_qc_states}, 4, 'retrieved QC states for four wells');
  lives_ok { $qc_loader->load_qc_state() } 'loaded QC states for four wells';
  my $num_updated_rows =
    $rs_92->search({'qc_seq_state' => {'!=', undef}})->count();
  is ($num_updated_rows, 4, 'four rows are updated');

  my $input = decode_json($json_92);
  foreach my $product_id (keys %{$input}) {
    my $row = $rs_92->find({id_pac_bio_product => $product_id});
    _test_row_update($input->{$product_id}->[0], $row);
  }

  my @products = $rs_92->search({'well_label' => 'D1'})
                 ->search_related_rs('pac_bio_product_metrics', {})->all();
  is (@products, 40, '40 product rows for well D1');
  my @qc = uniq map { $_->qc } @products; 
  is (@qc, 1, 'one unique value for all product QC results');
  is ($qc[0], 1, 'pass is recorded');
};

sub _test_row_update {
  my ($input, $row) = @_;

  my $qc_seq = defined $input->{outcome} ? ($input->{outcome}? 1 : 0) : undef;
  is ($row->qc_seq_state, $input->{qc_state}, 'qc state is correct');
  is ($row->qc_seq_state_is_final, $input->{is_preliminary} ? 0 : 1,
    'finality is set correctly');
  is ($row->qc_seq_date, $input->{date_updated}, 'date is correct');
  is ($row->qc_seq, $qc_seq, 'seq qc outcome is set correctly');

  my $prs = $row->pac_bio_product_metrics();
  while (my $prow = $prs->next) {
    is ($prow->qc, $row->qc_seq_state_is_final ? $row->qc_seq : undef,
    'product qc is set (or not) correctly');
  }
}

1;
