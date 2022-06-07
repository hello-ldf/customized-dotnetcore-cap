// Copyright (c) .NET Core Community. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using DotNetCore.CAP.Internal;
using DotNetCore.CAP.Persistence;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace DotNetCore.CAP.Processor
{
    public class MessageNeedToRetryProcessor : IProcessor
    {
        private readonly TimeSpan _delay = TimeSpan.FromSeconds(1);
        private readonly ILogger<MessageNeedToRetryProcessor> _logger;
        private readonly IMessageSender _messageSender;
        private readonly ISubscribeDispatcher _subscribeDispatcher;
        private readonly TimeSpan _waitingInterval;

        public MessageNeedToRetryProcessor(
            IOptions<CapOptions> options,
            ILogger<MessageNeedToRetryProcessor> logger,
            ISubscribeDispatcher subscribeDispatcher,
            IMessageSender messageSender)
        {
            _logger = logger;
            _subscribeDispatcher = subscribeDispatcher;
            _messageSender = messageSender;
            _waitingInterval = TimeSpan.FromSeconds(options.Value.FailedRetryInterval);
        }

        public virtual async Task ProcessAsync(ProcessingContext context)
        {
            if (context == null)
            {
                throw new ArgumentNullException(nameof(context));
            }

            var storage = context.Provider.GetRequiredService<IDataStorage>();

            await Task.WhenAll(ProcessPublishedAsync(storage, context), ProcessReceivedAsync(storage, context));

            await context.WaitAsync(_waitingInterval);
        }

        private async Task ProcessPublishedAsync(IDataStorage connection, ProcessingContext context)
        {
            context.ThrowIfStopping();

            IEnumerable<MediumMessage> messages;
            long skip = 0, take = 200;

            //do
            //{
            messages = await GetSafelyAsync(async () => await connection.GetPublishedMessagesOfNeedRetry(skip, take));

            int groupCount = 20, i = 0;
            while (true)
            {
                var group = messages.Skip(i * groupCount).Take(groupCount);
                var tasks = group.Select(async u => await _messageSender.SendAsync(u));
                await Task.WhenAll(tasks);

                if (group.Count() < groupCount)
                    break;

                await context.WaitAsync(_delay);
                i++;
            }

            //skip += take;
            //} while (messages.Count() == take);
        }

        private async Task ProcessReceivedAsync(IDataStorage connection, ProcessingContext context)
        {
            context.ThrowIfStopping();

            IEnumerable<MediumMessage> messages;
            long skip = 0, take = 200;

            //do
            //{
            messages = await GetSafelyAsync(async () => await connection.GetReceivedMessagesOfNeedRetry(skip, take));

            int groupCount = 20, i = 0;
            while (true)
            {
                var group = messages.Skip(i * groupCount).Take(groupCount);
                var tasks = group.Select(async u => await _subscribeDispatcher.DispatchAsync(u, context.CancellationToken));
                await Task.WhenAll(tasks);

                if (group.Count() < groupCount)
                    break;

                await context.WaitAsync(_delay);
                i++;
            }
            //skip += take;
            //} while (messages.Count() == take);
        }

        private async Task<IEnumerable<T>> GetSafelyAsync<T>(Func<Task<IEnumerable<T>>> getMessagesAsync)
        {
            try
            {
                return await getMessagesAsync();
            }
            catch (Exception ex)
            {
                _logger.LogWarning(1, ex, "Get messages from storage failed. Retrying...");

                return Enumerable.Empty<T>();
            }
        }
    }
}