using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;

namespace Sample.AzureStorageTable.Handler
{
    public class TableProxyHandler : DelegatingHandler
    {
        public int CallCount { get; private set; }

        private readonly IWebProxy _proxy;

        private bool _firstCall = true;

        public TableProxyHandler() : base()
        {

        }

        public TableProxyHandler(HttpMessageHandler httpMessageHandler) : base(httpMessageHandler)
        {

        }

        public TableProxyHandler(IWebProxy proxy)
        {
            _proxy = proxy;
        }

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            CallCount++;
            if (_firstCall && _proxy != null)
            {
                var inner = (HttpClientHandler)InnerHandler;
                inner.Proxy = _proxy;
            }
            _firstCall = false;
            return base.SendAsync(request, cancellationToken);
        }
    }
}
