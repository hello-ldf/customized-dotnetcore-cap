// Copyright (c) .NET Core Community. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Threading.Tasks;
using DotNetCore.CAP.Messages;

namespace DotNetCore.CAP.Transport
{
    public interface ITransport
    {
        BrokerAddress BrokerAddress { get; }

        Task<OperateResult> SendAsync(TransportMessage message);

        void SubscribeDynamic([NotNull] IEnumerable<string> topics);

        void UnsubscribeDynamic([NotNull] IEnumerable<string> topics);
    }
}
