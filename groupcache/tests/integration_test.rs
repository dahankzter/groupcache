mod common;

use anyhow::Result;
use common::*;
use groupcache::{Groupcache, GroupcachePeer};
use pretty_assertions::assert_eq;
use std::collections::HashSet;

use std::net::SocketAddr;
use std::ops::Sub;
use tokio::time;

#[tokio::test]
async fn test_when_there_is_only_one_peer_it_should_handle_entire_key_space() -> Result<()> {
    let groupcache = {
        let loader = TestCacheLoader::new("1");
        let addr: SocketAddr = "127.0.0.1:8080".parse()?;
        Groupcache::<CachedValue>::builder(addr.into(), loader).build()
    };

    let key = "K-some-random-key-d2k";
    successful_get(key, Some("1"), groupcache.clone()).await;
    successful_get(key, Some("1"), groupcache.clone()).await;

    let error_key = "Key-triggering-loading-error";
    let err = groupcache
        .get(error_key)
        .await
        .expect_err("expected error from loader");

    assert_eq!(
        "Loading error: 'Something bad happened during loading :/'",
        err.to_string()
    );

    Ok(())
}

#[tokio::test]
async fn test_set_peers_updates_routing_table() -> Result<()> {
    let peers = spawn_instances(5).await?;

    let (me, others) = peers.split_first().unwrap();
    let peers = others
        .iter()
        .map(|e| e.addr().into())
        .collect::<HashSet<GroupcachePeer>>();

    me.set_peers(peers.clone()).await?;
    for i in 1..5 {
        let peer = others[i - 1].clone();
        let key = key_owned_by_instance(peer);
        let peer_index = &*i.to_string();
        successful_get(&key, Some(peer_index), me.clone()).await;
    }

    let tbr: HashSet<GroupcachePeer> = vec![others[3].addr().into()].into_iter().collect();
    let subset = peers.sub(&tbr);
    me.set_peers(subset.clone()).await?;

    let key = key_owned_by_instance(others[3].clone());
    successful_get_not_from_instance(&key, "4", me.clone()).await;

    Ok(())
}

#[tokio::test]
async fn test_healthy_peers_added_despite_one_unhealthy() -> Result<()> {
    let peers = spawn_instances(5).await?;

    let (me, others) = peers.split_first().unwrap();
    let mut peers = others
        .iter()
        .map(|e| e.addr().into())
        .collect::<HashSet<GroupcachePeer>>();

    // Bind a port, then immediately drop the listener so the port is free
    // but nothing is listening. This avoids flakiness from hardcoded ports
    // that may be occupied by other processes.
    let unreachable_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let unreachable_addr = unreachable_listener.local_addr()?;
    drop(unreachable_listener);
    peers.insert(unreachable_addr.into());
    let result = me.set_peers(peers.clone()).await;
    if result.is_ok() {
        panic!("Expected failure to set peers with unreachable address");
    }

    for i in 1..5 {
        let peer = others[i - 1].clone();
        let key = key_owned_by_instance(peer);
        let peer_index = &*i.to_string();
        successful_get(&key, Some(peer_index), me.clone()).await;
    }

    Ok(())
}

#[tokio::test]
async fn test_when_peers_are_healthy_they_should_respond_to_queries() -> Result<()> {
    let (instance_one, instance_two) = two_connected_instances().await?;

    for &key in ["K-b3a", "K-karo", "K-x3d", "K-d42", "W-a1a"].iter() {
        successful_get(key, None, instance_one.clone()).await;
        successful_get(key, None, instance_two.clone()).await;
    }

    Ok(())
}

#[tokio::test]
async fn test_when_peer_disconnects_requests_should_fail_with_transport_error() -> Result<()> {
    let (instance_one, instance_two) = two_instances_with_one_disconnected().await?;
    for &key in ["K-b3a", "K-karo", "K-x3d", "K-d42", "W-a1a"].iter() {
        success_or_transport_err(key, instance_one.clone()).await;
        success_or_transport_err(key, instance_two.clone()).await;
    }

    Ok(())
}

#[ignore]
#[tokio::test]
async fn test_when_peer_reconnects_it_should_respond_to_queries() -> Result<()> {
    let (instance_one, instance_two) = two_instances_with_one_disconnected().await?;
    reconnect(instance_two.clone()).await;

    let key_on_instance_2 = key_owned_by_instance(instance_two.clone());
    success_or_transport_err(&key_on_instance_2, instance_one.clone()).await;
    successful_get(&key_on_instance_2, Some("2"), instance_one.clone()).await;

    Ok(())
}

#[tokio::test]
async fn when_new_peer_joins_it_should_receive_requests() -> Result<()> {
    let (instance_one, instance_two) = two_instances().await?;
    let key_on_instance_2 = key_owned_by_instance(instance_two.clone());
    successful_get(&key_on_instance_2, Some("2"), instance_two.clone()).await;

    instance_one.add_peer(instance_two.addr().into()).await?;
    successful_get(&key_on_instance_2, Some("2"), instance_one.clone()).await;

    Ok(())
}

#[tokio::test]
async fn test_when_remote_get_fails_during_load_then_load_locally() -> Result<()> {
    let (instance_one, instance_two) = two_connected_instances().await?;
    let key_on_instance_2 = error_key_on_instance(instance_two.clone());

    let err = instance_one
        .get(&key_on_instance_2)
        .await
        .expect_err("expected error from loader");

    assert_eq!(
        "Loading error: 'Something bad happened during loading :/'",
        err.to_string()
    );

    Ok(())
}

#[tokio::test]
async fn test_when_peer_is_removed_traffic_is_no_longer_routed_to_it() -> Result<()> {
    let (instance_one, instance_two) = two_connected_instances().await?;
    instance_one
        .remove_peer(GroupcachePeer::from_socket(instance_two.addr()))
        .await?;
    let key = key_owned_by_instance(instance_two.clone());

    successful_get(&key, Some("1"), instance_one.clone()).await;
    Ok(())
}

#[tokio::test]
async fn test_when_there_are_multiple_instances_each_should_own_portion_of_key_space() -> Result<()>
{
    let instances = spawn_instances(10).await?;
    let first_instance = instances[0].clone();

    for (i, instance) in instances.iter().enumerate() {
        let key_on_instance = key_owned_by_instance(instance.clone());
        successful_get(
            &key_on_instance,
            Some(&i.to_string()),
            first_instance.clone(),
        )
        .await;
    }

    Ok(())
}

#[tokio::test]
async fn when_kv_is_loaded_it_should_be_cached_by_the_owner() -> Result<()> {
    let (instance_one, instance_two) = two_connected_instances().await?;
    let key_on_instance_2 = key_owned_by_instance(instance_two.clone());

    successful_get_opts(
        &key_on_instance_2,
        instance_one.clone(),
        GetAssertions {
            expected_instance_id: Some("2".into()),
            expected_load_count: Some(1),
            ..GetAssertions::default()
        },
    )
    .await;

    successful_get_opts(
        &key_on_instance_2,
        instance_two.clone(),
        GetAssertions {
            expected_instance_id: Some("2".into()),
            expected_load_count: Some(1),
            ..GetAssertions::default()
        },
    )
    .await;

    Ok(())
}

#[tokio::test]
async fn when_kv_is_loaded_it_should_be_cached_in_hot_cache() -> Result<()> {
    let (instance_one, instance_two) = two_connected_instances().await?;
    let key_on_instance_2 = key_owned_by_instance(instance_two.clone());

    successful_get_opts(
        &key_on_instance_2,
        instance_one.clone(),
        GetAssertions {
            expected_instance_id: Some("2".into()),
            expected_load_count: Some(1),
            ..GetAssertions::default()
        },
    )
    .await;

    instance_two.remove(&key_on_instance_2).await?;

    successful_get_opts(
        &key_on_instance_2,
        instance_one.clone(),
        GetAssertions {
            expected_instance_id: Some("2".into()),
            expected_load_count: Some(1),
            ..GetAssertions::default()
        },
    )
    .await;

    Ok(())
}

#[tokio::test]
async fn when_kv_is_saved_in_hot_cache_it_should_expire_according_to_ttl() -> Result<()> {
    let (instance_one, instance_two) = two_connected_instances().await?;
    let key_on_instance_2 = key_owned_by_instance(instance_two.clone());

    successful_get_opts(
        &key_on_instance_2,
        instance_one.clone(),
        GetAssertions {
            expected_instance_id: Some("2".into()),
            expected_load_count: Some(1),
            ..GetAssertions::default()
        },
    )
    .await;

    instance_two.remove(&key_on_instance_2).await?;
    time::sleep(HOT_CACHE_TTL).await;

    successful_get_opts(
        &key_on_instance_2,
        instance_one.clone(),
        GetAssertions {
            expected_instance_id: Some("2".into()),
            expected_load_count: Some(2),
            ..GetAssertions::default()
        },
    )
    .await;

    Ok(())
}

#[tokio::test]
async fn when_key_is_removed_then_it_should_be_removed_from_owner() -> Result<()> {
    let (instance_one, instance_two) = two_connected_instances().await?;
    let key_on_instance_2 = key_owned_by_instance(instance_two.clone());

    successful_get_opts(
        &key_on_instance_2,
        instance_one.clone(),
        GetAssertions {
            expected_instance_id: Some("2".into()),
            expected_load_count: Some(1),
            ..GetAssertions::default()
        },
    )
    .await;

    instance_one.remove(&key_on_instance_2).await?;

    successful_get_opts(
        &key_on_instance_2,
        instance_two.clone(),
        GetAssertions {
            expected_instance_id: Some("2".into()),
            expected_load_count: Some(2),
            ..GetAssertions::default()
        },
    )
    .await;

    Ok(())
}

#[tokio::test]
async fn test_add_peer_is_idempotent() -> Result<()> {
    let (instance_one, instance_two) = two_connected_instances().await?;

    // Adding the same peer again should be a no-op (not an error).
    instance_one
        .add_peer(GroupcachePeer::from_socket(instance_two.addr()))
        .await?;

    // Routing should still work correctly.
    let key = key_owned_by_instance(instance_two.clone());
    successful_get(&key, Some("2"), instance_one.clone()).await;

    Ok(())
}

#[tokio::test]
async fn test_set_peers_connects_to_genuinely_new_peers() -> Result<()> {
    // Create instances WITHOUT connecting them through spawn_instances.
    let instance_one = spawn_groupcache("1").await?;
    let instance_two = spawn_groupcache("2").await?;
    let instance_three = spawn_groupcache("3").await?;

    // Use set_peers to introduce both new peers at once.
    let peers: HashSet<GroupcachePeer> = [instance_two.addr(), instance_three.addr()]
        .into_iter()
        .map(GroupcachePeer::from_socket)
        .collect();

    instance_one.set_peers(peers).await?;

    // Verify routing works through the newly connected peers.
    let key_on_two = key_owned_by_instance(instance_two.clone());
    successful_get(&key_on_two, Some("2"), instance_one.clone()).await;

    let key_on_three = key_owned_by_instance(instance_three.clone());
    successful_get(&key_on_three, Some("3"), instance_one.clone()).await;

    Ok(())
}

#[tokio::test]
async fn test_remove_from_disconnected_peer_returns_transport_error() -> Result<()> {
    let (instance_one, instance_two) = two_instances_with_one_disconnected().await?;

    // Find a key owned by the disconnected instance_two.
    let key = key_owned_by_instance(instance_two.clone());

    // The graceful shutdown keeps existing HTTP/2 connections alive briefly.
    // Retry remove until the connection actually drops.
    let mut last_err = None;
    for _ in 0..20 {
        match instance_one.remove(&key).await {
            Err(e) => {
                last_err = Some(e);
                break;
            }
            Ok(()) => {
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
        }
    }

    let err = last_err.expect("expected transport error for remove on disconnected peer");
    let err_string = err.to_string();
    assert!(
        err_string.contains("Transport"),
        "expected Transport error, got: '{}'",
        err_string
    );

    Ok(())
}

#[tokio::test]
async fn test_remove_peer_that_is_not_known_is_noop() -> Result<()> {
    let instance = single_instance().await?;
    let unknown_addr: SocketAddr = "127.0.0.1:19999".parse()?;

    // Removing a peer that was never added should succeed silently.
    instance
        .remove_peer(GroupcachePeer::from_socket(unknown_addr))
        .await?;

    Ok(())
}
