package npg_warehouse::loader::illumina::autoqc;

use Carp;
use Moose;
use MooseX::StrictConstructor;
use Readonly;
use Clone qw/clone/;

use npg_tracking::glossary::rpt;
use npg_tracking::glossary::composition;
use npg_tracking::glossary::composition::component::illumina;
use npg_qc::autoqc::qc_store;
use npg_qc::autoqc::qc_store::options qw/$ALLALL/;
use npg_qc::autoqc::qc_store::query;
use npg_qc::autoqc::results::collection;

our $VERSION = '0';

## no critic (ProhibitUnusedPrivateSubroutines)

Readonly::Scalar our $PP_KEY     => q[pp];

# Maximum value for MYSQL smallint unsigned
Readonly::Scalar my $INSERT_SIZE_QUARTILE_MAX_VALUE => 65_535;
Readonly::Scalar my $HUNDRED  => 100;
Readonly::Scalar my $PRIMER_PANEL_MAX_LENGTH => 255;

Readonly::Hash   my %AUTOQC_MAPPING  => {
     gc_fraction =>      {
                           'gc_percent_forward_read' => 'forward_read_gc_percent',
                           'gc_percent_reverse_read' => 'reverse_read_gc_percent',
                         },
     sequence_error =>   {
                           'sequence_mismatch_percent_forward_read' => 'forward_average_percent_error',
                           'sequence_mismatch_percent_reverse_read' => 'reverse_average_percent_error',
                         },
     adapter     =>      {
                           'adapters_percent_forward_read' => 'forward_percent_contam_reads',
                           'adapters_percent_reverse_read' => 'reverse_percent_contam_reads',
                         },
     pulldown_metrics => { 'mean_bait_coverage'      => 'mean_bait_coverage',
                           'mean_bait_target_coverage' => 'mean_target_coverage',
                           'on_bait_percent'         => 'on_bait_bases_percent',
                           'on_or_near_bait_percent' => 'selected_bases_percent',
                         },
     verify_bam_id    => { 'verify_bam_id_score'         => 'freemix',
                           'verify_bam_id_average_depth' => 'avg_depth',
                           'verify_bam_id_snp_count'     => 'number_of_snps',
                         },
     rna_seqc         => {
                           'rna_exonic_rate'               => 'exonic_rate',
                           'rna_percent_end_2_reads_sense' => 'end_2_pct_sense',
                           'rna_rrna_rate'                 => 'rrna_rate',
                           'rna_genes_detected'            => 'genes_detected',
                           'rna_norm_3_prime_coverage'     => 'end_3_norm',
                           'rna_norm_5_prime_coverage'     => 'end_5_norm',
                           'rna_intronic_rate'             => 'intronic_rate',
                           'rna_transcripts_detected'      => 'transcripts_detected',
                           'rna_globin_percent_tpm'        => 'globin_pct_tpm',
                           'rna_mitochondrial_percent_tpm'  => 'mt_pct_tpm',
                         },
     genotype_call    => {
                           'gbs_call_rate'                 => 'genotype_call_rate',
                           'gbs_pass_rate'                 => 'genotype_passed_rate',
                         },
     bcfstats         => {
                           'nrd_percent'                   => 'percent_nrd',
                         },
     substitution_metrics => {'sub_titv_class'   => 'titv_class',
                              'sub_titv_mean_ca' => 'titv_mean_ca',
                              'sub_frac_sub_hq'  => 'frac_sub_hq',
                              'sub_oxog_bias'    => 'oxog_bias',
                              'sub_sym_gt_ca'    => 'sym_gt_ca',
                              'sub_sym_ct_ga'    => 'sym_ct_ga',
                              'sub_sym_ag_tc'    => 'sym_ag_tc',
                              'sub_cv_ti'        => 'cv_ti',
                              'sub_gt_ti'        => 'gt_ti',
                              'sub_gt_mean_ti'   => 'gt_mean_ti',
                              'sub_ctoa_oxh'     => 'ctoa_oxh',
                              'sub_ctoa_art_predicted_level' => 'ctoa_art_predicted_level',
                             },
};

=head1 NAME

npg_warehouse::loader::illumina::autoqc

=head1 SYNOPSIS
 
 my $id_run = 222;
 my $autoqc_hash = npg_:warehouse::loader::illumina::autoqc->new()->retrieve($id_run);

=head1 DESCRIPTION

Retrieval of autoqc data for loading to the warehouse

=head1 SUBROUTINES/METHODS

=head2 autoqc_store

A driver to retrieve autoqc objects, required attribute.

=cut
has 'autoqc_store' => ( isa      => 'npg_qc::autoqc::qc_store',
                        is       => 'ro',
                        required => 1,
                      );

=head2 interop_data_column_names

An array reference of column names for Illumina InterOp data.

=cut
has 'interop_data_column_names' => ( isa     => 'ArrayRef',
                                     is      => 'ro',
                                     default => sub {return []},
                                   );

sub _basic_data {
    my ($self, $composition) = @_;
    $composition or croak 'Composition argument needed';
    return {'composition' => $composition};
}

sub _truncate_float {
    my ($self, $value) = @_;
    ##no critic (ProhibitEscapedMetacharacters ProhibitEnumeratedClasses)
    if ($value && $value =~ /[0-9]*\.[0-9]+/smx) {
        $value = sprintf '%.2f', $value;
    }
    return $value;
}

sub _compositions4tags {
    my ($self, $id_run, $position, $tags) = @_;

    my @compositions = ();
    foreach my $tag (@{$tags}) {
      push @compositions,
        npg_tracking::glossary::composition->new(components => [
          npg_tracking::glossary::composition::component::illumina
          ->new(id_run => $id_run, position => $position, tag_index => $tag)
        ]);
    }

    return \@compositions;
}

sub _composition_without_subset {
    my ($self, $composition) = @_;

    my @components =
        map { npg_tracking::glossary::composition::component::illumina->new($_) }
        map { npg_tracking::glossary::rpt->inflate_rpt($_->freeze2rpt) }
        $composition->components_list();

    return npg_tracking::glossary::composition->new(components => \@components);
}

sub _astats_data {
    my ($astats, $info, $common_data) = @_;

    my $num_amplicons = $astats->{num_amplicons};
    $num_amplicons or croak 'Number of amplicons should be defined';
    my $command = $info->{Samtools_command};
    $command or croak 'Samtools_command is not recorded';
    my ($primer_panel) = $command =~ /primer_panel\/(\S+[.]bed)\s*\S*\Z/smx;
    $primer_panel or
      ($primer_panel) = $command =~ /(\S+[.]bed)\s*\S*\Z/smx;
    $primer_panel or croak 'Failed to extract the primer panel path';
    # Trim the start of the string to fit the column.
    $primer_panel = substr $primer_panel, -$PRIMER_PANEL_MAX_LENGTH;

    $common_data->{primer_panel} = $primer_panel;
    $common_data->{primer_panel_num_amplicons} = $num_amplicons;

    my $convert_name = sub {
        my $name = shift;
        $name =~ s/-/_/gsmx;
        return join q[_], 'metric', lc $name;
    };

    my @per_amplicon_data = ();

    for my $i ((1 .. $num_amplicons)) {
        my $idata = clone($common_data);
        $idata->{amplicon_index} = $i;
        for my $name ( keys %{$astats} ) {
            my $array = $astats->{$name};
            $array and (ref $array eq q[ARRAY]) or next;
            my $value = $array->[$i-1];
            defined $value or croak 'Array length mismatch';
            $idata->{ $convert_name->($name) } = $value;
        }
        push @per_amplicon_data, $idata;
    }

    return \@per_amplicon_data;
}

sub _generic {
    my ($self, $result, $c) = @_;

    $result->pp_name or croak 'pp_name attribute should be defined';
    my $basic_data = $self->_basic_data($c);
    my $data = {};
    $data->{'pp_name'}    = $result->pp_name;
    $data->{'pp_version'} = $result->info->{'Pipeline_version'};

    if ($result->pp_name eq 'ncov2019-artic-nf') {
        $data->{'pp_repo_url'} = $result->info->{'Pipeline_repo_url'};

        my @names =
            map { $_ eq 'ivar' ? 'ivar_md' : $_ }
            map { ($_, $_ . '_amd') }
            qw/ivar longest_no_N_run pct_N_bases/;
        push @names, qw/pct_covered_bases num_aligned_reads qc_pass/;

        foreach my $name (@names) {
            my $pname = $name eq 'qc_pass' ? 'artic_qc_outcome' : lc $name;
            $data->{$pname} = $result->doc->{'QC summary'}->{$name};
        }

        my $key = 'supplier_sample_name';
        $data->{$key} = $result->doc->{'meta'}->{$key};
        $basic_data->{$PP_KEY} = {$result->pp_name => $data};

    } elsif ($result->pp_name =~ /ampliconstats/xms) {
        my $astats = $result->doc->{amplicon_stats};
        if ($astats and keys %{$astats}) {
            $basic_data->{$PP_KEY} =
                {$result->pp_name => _astats_data($astats, $result->info, $data)};
        }
    }

    return $basic_data->{$PP_KEY} ? ($basic_data) : ();
}

sub _interop {
    my ($self, $result, $c) = @_;

    @{$self->interop_data_column_names()} or croak 'Interop column names should be set';

    my $data = $self->_basic_data($c);
    for my $name (@{$self->interop_data_column_names()}) {
        my $method_name = $name;
        $method_name =~ s/\Ainterop_//xms;
        my $value = $result->$method_name;
        # Some of the values are hashes, they'll have to be transformed.
        # Error for no so that we do not populate the database with addresses
        # of hash references.
        ref $value and croak "Cannot copy non-scalar $method_name value";
        $data->{$name} = $value;
    }

    return ($data);
}

sub _insert_size {
    my ($self, $result, $c) = @_;

    my $data = $self->_basic_data($c);

    my %h = qw/ insert_size_quartile1 quartile1
                insert_size_quartile3 quartile3
                insert_size_median median /;

    while (my ($column_name, $attr_name) = each %h) {
        my $value = $result->$attr_name;
        if (defined $value) {
            if ($value > $INSERT_SIZE_QUARTILE_MAX_VALUE) {
                carp sprintf 'SKIPPING %s for %s: value %i out of range',
                $attr_name, $c->freeze, $value;
                next;
            }
            $data->{$column_name} = $value;
        }
    }

    $data->{'insert_size_num_modes'} = $result->norm_fit_nmode;
    my $v = $result->norm_fit_confidence;
    if (defined $v) {
        if ($v > 1) {
            $v = 1;
        } elsif ($v < 0) {
            $v = 0;
        }
        $data->{'insert_size_normal_fit_confidence'} = $self->_truncate_float($v);
    }

    return ($data);
}

sub _qX_yield {
    my ($self, $result, $c) = @_;

    my $data = $self->_basic_data($c);
    foreach my $read (qw/1 2/) {
        foreach my $quality (qw/20 30 40/) {
            my $autoqc_method_name = sprintf 'yield%s_q%s', $read, $quality;
            my $wh_column_name     = sprintf 'q%s_yield_kb_%s_read',
                $quality, ($read eq '1') ? 'forward' : 'reverse';
            my $value = $result->$autoqc_method_name;
            if (defined $value) {
                $data->{$wh_column_name} = $result->$autoqc_method_name;
            }
        }
    }

    return ($data);
}

sub _ref_match {
    my ($self, $result, $c) = @_;

    my $data = $self->_basic_data($c);
    my $count = 0;
    foreach my $h ($result->top_two) {
        $count++;
        while (my ($key, $value) = each %{$h}) {
            $data->{q[ref_match] . $count.q[_] . $key} = $value;
        }
    }

    return ($count > 0 ? ($data) : ());
}

sub _tag_metrics {
    my ($self, $result, $composition) = @_;

    my $data = $self->_basic_data($composition);

    if (defined $result->matches_pf_percent) {
        $data->{'tags_decode_percent'} = $self->_truncate_float(
            $result->perfect_matches_percent + $result->one_mismatch_percent);
    }
    if (defined $result->variance_coeff) {
        $data->{'tags_decode_cv'} =
            $self->_truncate_float($result->variance_coeff(1));
    }
    if (defined $result->tag_hops_percent) {
        $data->{'tag_hops_percent'} = $result->tag_hops_percent;
    }
    if (defined $result->tag_hops_power) {
        $data->{'tag_hops_power'} = $result->tag_hops_power;
    }

    my @all = ($data);

    foreach my $c ( @{$self->_compositions4tags(
            $result->id_run, $result->position, [keys %{$result->tags}])} ) {
        my $d = $self->_basic_data($c);
        my $i = $c->get_component(0)->tag_index;
        if ($i != 0) { # no tag sequence for tag zero
            $d->{'tag_sequence'} = $result->tags->{$i};
        }
        $d->{'tag_decode_count'} = $result->reads_pf_count->{$i};
        $d->{'tag_decode_percent'} = $self->_truncate_float(
            $result->matches_pf_percent->{$i} * $HUNDRED);
        push @all, $d;
    }

    return @all;
}

sub _tag_decode_stats {
    my ($self, $result, $composition) = @_;

    my $data = $self->_basic_data($composition);
    if (defined $result->decoding_perc_good) {
        $data->{'tags_decode_percent'} =
            $self->_truncate_float($result->decoding_perc_good);
    }
    if (defined $result->variance_coeff) {
        $data->{'tags_decode_cv'} =
            $self->_truncate_float($result->variance_coeff('all'));
    }

    my @all = ($data);

    my $tags = $result->tag_code;
    if ($tags) {
    my $good       = $result->distribution_perc_good;
    my $good_count = $result->distribution_good;
        foreach my $c ( @{$self->_compositions4tags(
                $result->id_run, $result->position, [keys %{$tags}])} ) {
            my $d = $self->_basic_data($c);
            my $tag_index = $c->get_component(0)->tag_index;
            $d->{'tag_sequence'} = $tags->{$tag_index};
            if ($good && exists $good->{$tag_index}) {
                $d->{'tag_decode_percent'} =
                    $self->_truncate_float($good->{$tag_index});
                $d->{'tag_decode_count'} = $good_count->{$tag_index};
            }
            push @all, $d;
        }
    }

    return @all;
}

sub _bam_flagstats {
    my ($self, $result, $composition) = @_;

    my $subset = $composition->get_component(0)->subset();

    if ($subset && ($subset !~ /human/xsm)) {
      return ();
    }

    # Only no subset or flavours of human subset below
    my $c = $subset ?
        $self->_composition_without_subset($composition) : $composition;
    my $data = $self->_basic_data($c);
    #####
    # Separate columns for a 'human' split data might have been originally
    # created for pathogen studies. Eventually the data for y and xa
    # splits got saved to these columns. So the names of these colums
    # are now slightly misleading.
    my $column_name_prefix = $subset ? 'human_' : q[];
    $column_name_prefix = q[bam_] . $column_name_prefix;

    foreach my $m (qw(percent_mapped percent_duplicate)) {
        my $method = join q[_], $m, 'reads';
        if (my $r = $result->$method ) {
            $data->{$column_name_prefix . $m} = $r;
        }
    }

    if (not $subset) { # No splits below here
        my $num_reads = $result->total_reads;
        $data->{'bam_num_reads'} = $num_reads;
        my $chimeric_reads = $self->_truncate_float(
            ($num_reads && $result->mate_mapped_defferent_chr_5)
            ? ($result->mate_mapped_defferent_chr_5 * $HUNDRED / $num_reads)
            : 0.00);
        $data->{'chimeric_reads_percent'} = $chimeric_reads;
        foreach my $method ( map { 'target_' . $_ } qw(
                filter length mapped_reads mapped_bases
                proper_pair_mapped_reads coverage_threshold 
                percent_gt_coverage_threshold
                autosome_filter autosome_length
                autosome_mapped_bases
                autosome_coverage_threshold
                autosome_percent_gt_coverage_threshold
                )) {
            if (my $r = $result->$method ) {
                $data->{$method} = $r;
            }
        }
    }

    return ($data);
}

sub _upstream_tags {
    my ($self, $result, $c) = @_;

    my $data = $self->_basic_data($c);
    my $total = $result->total_lane_reads;
    my $unexpected_tags_percent = $self->_truncate_float(
        $total ? $result->tag0_perfect_match_reads * $HUNDRED / $total : 0.00);
    $data->{'unexpected_tags_percent'} = $unexpected_tags_percent;

    return ($data);
}

sub _genotype {
    my ($self, $result, $composition) = @_;

    my $data = $self->_basic_data($composition);

    if (defined $result->sample_name_match) {
        # Probably, the data can be fixed instead of setting to 0 if false
        $data->{'genotype_sample_name_match'} = join q[/],
            $result->sample_name_match->{'match_count'}      || 0,
            $result->sample_name_match->{'common_snp_count'} || 0;
    }

    if (defined $result->sample_name_relaxed_match) {
        $data->{'genotype_sample_name_relaxed_match'} = join q[/],
            $result->sample_name_relaxed_match->{'match_count'},
            $result->sample_name_relaxed_match->{'common_snp_count'};
    }

    my $bam_gt_depths_string = $result->bam_gt_depths_string;
    if (defined $bam_gt_depths_string) {
        my $tot = 0;
        my $c = 0;
        for my $v (split /;/smx, $bam_gt_depths_string) {
            $tot += $v;
            $c++;
        }
        $data->{'genotype_mean_depth'} = sprintf '%.02f', ($tot / $c);
    }

    return ($data);
}

sub _autoqc_check {
    my ($self, $result, $c) = @_;

    my $subset = $c->get_component(0)->subset();
    if ($subset) {
      return ();
    }

    my $data = $self->_basic_data($c);
    my $map = $AUTOQC_MAPPING{$result->class_name};
    foreach my $key (keys %{$map}) {
        my $method = $map->{$key};
        my $value = $result->$method;
        if (defined $value) {
            if ( $key !~ /\Averify_bam_id|\Arna/xms ) {
                $value = $self->_truncate_float($value);
            }
            $data->{$key} = $value;
        }
    }

    return ($data);
}

sub _add_data {
    my ($self, $autoqc, $data, $digest) = @_;

    if (exists $autoqc->{$digest}) {
        delete $data->{'composition'};
        while (my ($column_name, $value) = each %{$data}) {
            if (ref $value eq 'HASH') {
                ($column_name eq $PP_KEY) or croak "Unexpected key $column_name";
                my @keys = keys %{$value};
                (@keys == 1) or croak 'Invalid number of keys';
                my $key = $keys[0];
                # Be careful, do not overwrite data from other pipelines, which
                # migh be already hashed under $PP_KEY.
                $autoqc->{$digest}->{$column_name}->{$key} = $value->{$key};
            } else {
                $autoqc->{$digest}->{$column_name} = $value;
            }
        }
    } else {
        $autoqc->{$digest} = $data;
    }

    return;
}

=head2 retrieve

Retrieves autoqc results for a run.

=cut

sub retrieve {
    my ($self, $id_run, $npg_schema) = @_;

    my $query = npg_qc::autoqc::qc_store::query->new(
                                                id_run              => $id_run,
                                                option              => $ALLALL,
                                                npg_tracking_schema => $npg_schema
                                                    );
    my $collection = $self->autoqc_store->load($query);
    return $self->process($collection);
}

=head2 process

Process collection to produce data suitable for loading to the warehouse. 

=cut

sub process {
    my ($self, $collection) = @_;

    my $hashed = {};
    my $methods = {};
    foreach my $r (@{$collection->results}) {
      my $class_name = $r->class_name;
      my $method_name = exists $AUTOQC_MAPPING{$class_name}
                        ? q[_autoqc_check] : q[_] . $class_name;
      if ($self->can($method_name)) {
        $methods->{$class_name} = $method_name;
        push @{$hashed->{$r->composition_digest}->{$class_name}}, $r;
      }
    }

    my $autoqc = {};
    foreach my $digest (keys %{$hashed}) {
      if ( $hashed->{$digest}->{'tag_decode_stats'} &&
           $hashed->{$digest}->{'tag_metrics'} ) {
        delete $hashed->{$digest}->{'tag_decode_stats'};
      }
      my @class_names = keys %{$hashed->{$digest}};
      my $r = $hashed->{$digest}->{$class_names[0]}->[0];
      my $composition = $r->composition;
      foreach my $class_name (@class_names) {
        my $method_name = $methods->{$class_name};
        foreach my $result (@{$hashed->{$digest}->{$class_name}}) {
          foreach my $d ($self->$method_name($result, $composition)) {
            my $digest4wh = $d->{'composition'} eq $composition ? # the same object
                            $digest : $d->{'composition'}->digest();
            $self->_add_data($autoqc, $d, $digest4wh);
          }
        }
      }
    }

    return $autoqc;
}

__PACKAGE__->meta->make_immutable;

1;

__END__

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Carp

=item Readonly

=item Moose

=item MooseX::StrictConstructor

=item npg_tracking::glossary::rpt

=item npg_tracking::glossary::composition

=item npg_tracking::glossary::composition::component::illumina

=item npg_qc::autoqc::qc_store

=item npg_qc::autoqc::qc_store::options

=item npg_qc::autoqc::qc_store::query

=item npg_qc::autoqc::results::collection

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Marina Gourtovaia

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2018,2019,2020,2021 Genome Research Ltd.

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.

=cut
