using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Configuration;
using Amazon.ServiceDiscovery;
using Amazon.ServiceDiscovery.Model;

namespace Akka.Cluster.Discovery.AmazonEcsCloudmap
{
    public class AmazonCloudMapDiscoveryService : LocklessDiscoveryService
    {
        #region internal classes

        /// <summary>
        /// Message scheduled by <see cref="AmazonCloudMapDiscoveryService"/> for itself. 
        /// Used to trigger periodic restart of consul client.
        /// </summary>
        public sealed class RestartClient
        {
            public static RestartClient Instance { get; } = new RestartClient();

            private RestartClient()
            {
            }
        }

        #endregion

        private readonly AmazonCloudMapSettings settings;
        private IAmazonServiceDiscovery _serviceDiscovery;
        private readonly string protocol;

        public AmazonCloudMapDiscoveryService(Config config) : this(new AmazonCloudMapSettings(config))
        {
            protocol = ((ExtendedActorSystem) Context.System).Provider.DefaultAddress.Protocol;
        }

        public AmazonCloudMapDiscoveryService(AmazonCloudMapSettings settings)
            : this(CreateAmazonClient(settings), settings)
        {
        }

        public AmazonCloudMapDiscoveryService(IAmazonServiceDiscovery awsClient, AmazonCloudMapSettings settings) :
            base(settings)
        {
        }

        protected override void Ready()
        {
            base.Ready();
            Receive<RestartClient>(_ =>
            {
                Log.Debug("Restarting amazon service discovery client...");

                _serviceDiscovery = new AmazonServiceDiscoveryClient();
                _serviceDiscovery.Dispose();
                _serviceDiscovery = CreateAmazonClient(settings);
            });
        }

        protected override async Task<IEnumerable<Address>> GetNodesAsync(bool onlyAlive)
        {
            var discoverInstancesResult =
                await Task.WhenAll(settings.ServiceNames
                    .Select(serviceName => new DiscoverInstancesRequest
                    {
                        HealthStatus = onlyAlive ? HealthStatusFilter.HEALTHY : HealthStatusFilter.ALL,
                        NamespaceName = settings.NamespaceName,
                        ServiceName = serviceName
                    })
                    .Select(async request =>
                    {
                        try
                        {
                            var response = await _serviceDiscovery.DiscoverInstancesAsync(request);
                            return response.Instances.AsEnumerable();
                        }
                        catch (Exception)
                        {
                            return await Task.FromResult(Enumerable.Empty<HttpInstanceSummary>());
                        }
                    }));
        }

        protected override Task RegisterNodeAsync(MemberEntry node)
        {
            throw new NotImplementedException();
        }

        protected override Task DeregisterNodeAsync(MemberEntry node)
        {
            throw new NotImplementedException();
        }

        protected override Task MarkAsAliveAsync(MemberEntry node)
        {
            throw new NotImplementedException();
        }

        new NotImplementedException();

        private static AmazonServiceDiscoveryClient CreateAmazonClient(AmazonCloudMapSettings settings)
        {
            return new AmazonServiceDiscoveryClient(settings.RegionEndpoint);
        }
    }
}