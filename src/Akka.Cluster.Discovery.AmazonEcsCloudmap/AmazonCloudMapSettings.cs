using System;
using System.Collections.Generic;
using Akka.Configuration;
using Amazon;

namespace Akka.Cluster.Discovery.AmazonEcsCloudmap
{
    public class AmazonCloudMapSettings : LocklessClusterDiscoverySettings
    {
        public AmazonCloudMapSettings(Config config) : base(config)
        {
            RegionEndpoint = RegionEndpoint.GetBySystemName(config.GetString("region"));
            NamespaceName = config.GetString("namespace-name");
            ServiceNames = config.GetStringList("service-name");
            RestartInterval = !config.HasPath("restart-interval") ? default(TimeSpan?) : config.GetTimeSpan("restart-interval");

            var serviceCheckTtl = config.GetTimeSpan("service-check-ttl", new TimeSpan(this.AliveInterval.Ticks * 3));
            if (serviceCheckTtl < AliveInterval || serviceCheckTtl > AliveTimeout) throw new ArgumentException("`akka.cluster.discovery.consul.service-check-ttl` must greater than `akka.cluster.discovery.consul.alive-interval` and less than `akka.cluster.discovery.consul.alive-timeout`");
            
            ServiceCheckTtl = serviceCheckTtl;
        }

        public AmazonCloudMapSettings() : base()
        {
            RegionEndpoint = RegionEndpoint.APSoutheast1;
            ServiceCheckTtl = new TimeSpan(this.AliveInterval.Ticks * 3);
            RestartInterval = null;
        }

        public AmazonCloudMapSettings(RegionEndpoint regionEndpoint,
            string token,
            TimeSpan? waitTime,
            TimeSpan aliveInterval, 
            TimeSpan aliveTimeout, 
            TimeSpan refreshInterval,
            int joinRetries, 
            TimeSpan serviceCheckTtl,
            TimeSpan? restartInterval) 
            : base(aliveInterval, aliveTimeout, refreshInterval, joinRetries, DefaultTurnPeriod, DefaultMaxTurns)
        {
            if (serviceCheckTtl < AliveInterval || serviceCheckTtl > AliveTimeout) throw new ArgumentException("serviceCheckTtl must greater than aliveInterval and less than aliveTimeout", nameof(serviceCheckTtl));

            RegionEndpoint = regionEndpoint;
            ServiceCheckTtl = serviceCheckTtl;
            RestartInterval = restartInterval;
        }

        /// <summary>
        /// URL address on with CloudMap listener service can be found.
        /// </summary>
        public RegionEndpoint RegionEndpoint { get; }
        public string NamespaceName { get; }
        public IList<string> ServiceNames { get; }
        
        /// <summary>
        /// A timeout configured for consul to mark a time to live given for a node before it will be 
        /// marked as unhealthy. Must be greater than <see cref="ClusterDiscoverySettings.AliveInterval"/>
        /// and less than <see cref="ClusterDiscoverySettings.AliveTimeout"/>.
        /// </summary>
        public TimeSpan ServiceCheckTtl { get; }
        
        /// <summary>
        /// An interval in which consul client will be triggered for periodic restarts.
        /// If not provided or 0, client will never be restarted. Default value: null. 
        /// </summary>
        public TimeSpan? RestartInterval { get; }
    }
}