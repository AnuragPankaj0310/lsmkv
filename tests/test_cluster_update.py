from network.server import LsmkvServer


def test_cluster_update(tmp_path):
    server = LsmkvServer(
        host="127.0.0.1",
        port=7001,
        metrics_port=9001,
        data_dir=str(tmp_path),
        cluster_nodes=[
            "node1",
            "node2",
            "node3",
        ],
        node_address="node1",
    )

    assert len(server._ring.nodes) == 3

    server.update_cluster(
        [
            "node1",
            "node2",
            "node3",
            "node4",
        ]
    )

    assert len(server._ring.nodes) == 4