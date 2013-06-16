package Elasticsearch::NodePool::Sniff;

use strict;
use warnings;
use parent 'Elasticsearch::NodePool';
use namespace::autoclean;

use Elasticsearch::Error qw(throw);
use Try::Tiny;

# add max content?

#===================================
sub new {
#===================================
    my $self = shift()->SUPER::new(@_);
    $self->{original_nodes} = [ @{ $self->nodes } ];

    if ( $self->ping_on_first_use ) {
        $self->logger->debug("Force sniff on first request");
        $self->set_nodes();
    }

    return $self;
}

#===================================
sub default_args {
#===================================
    return (
        ping_interval_after_failure => 120,
        ping_on_first_use           => 1,
        should_accept_node          => sub {1}
    );
}

#===================================
sub next_node {
#===================================
    my $self = shift;

    my $nodes  = $self->nodes;
    my $now    = time();
    my $logger = $self->logger;
    my $debug  = $logger->is_debug;

    if ( @$nodes and $self->next_ping < $now ) {
        $logger->debug("Starting scheduled ping");
        $self->ping_nodes(@$nodes);
        $self->next_ping( $self->ping_interval );
    }

    if ( @$nodes == 0 ) {
        $logger->debug("Forced ping - no live nodes");
        $self->ping_nodes( @$nodes, @{ $self->original_nodes } );

        if ( @$nodes == 0 ) {
            $logger->throw_critical(
                "NoNodes",
                "No nodes are available: ",
                { nodes => $self->original_nodes }
            );
        }
        $self->next_ping( $self->ping_interval );
    }
    return $nodes->[ $self->next_node_num ];
}

#===================================
sub mark_dead {
#===================================
    my ( $self, $node ) = @_;
    $self->logger->debug("Marking node ($node) as dead");
    $self->set_nodes( grep { $_ ne $node } @{ $self->nodes } );
    $self->next_ping( $self->ping_interval_after_failure );
}

#===================================
sub ping_fail {
#===================================
    my ( $self, @nodes ) = @_;
    $self->logger->debugf( "Ping failed for nodes: %s", \@nodes );
}

#===================================
sub ping_success {
#===================================
    my ( $self, $node ) = @_;
    my $logger = $self->logger;
    $logger->debug("Retrieving live node list from node ($node)");

    my $cxn   = $self->connection;
    my $nodes = try {
        my $raw = $cxn->perform_request(
            $node,
            {   method => 'GET',
                path   => '/_cluster/nodes',
                qs     => { timeout => 300 }
            }
        );
        return $self->serializer->decode($raw)->{nodes};
    }
    catch {
        $logger->warn("$_");
        0;
    } or return;

    my $protocol_key = $cxn->protocol . '_address';

    my @live_nodes;
    for my $node_id ( keys %$nodes ) {
        my $data = $nodes->{$node_id};
        my $host = $data->{$protocol_key} or next;
        $host =~ s{^inet\[/([^\]]+)\]}{$1} or next;
        $self->should_accept_node( $host, $node_id, $data ) or next;
        push @live_nodes, $host;
    }

    unless (@live_nodes) {
        $logger->warn("No live nodes returned from node ($node)");
        return;
    }

    $self->set_nodes(@live_nodes);
    return 1;
}

#===================================
sub ping_interval_after_failure { $_[0]->{ping_interval_after_failure} }
sub original_nodes              { $_[0]->{original_nodes} }
sub should_accept_node          { shift()->{should_accept_node}->(@_) }
#===================================
1;