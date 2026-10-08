using AllLive.Core.Helper;
using AllLive.Core.Interface;
using AllLive.Core.Models;
using Newtonsoft.Json.Linq;
using System;
using System.IO;
using System.Net.WebSockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace AllLive.Core.Danmaku
{
    public sealed class DouyuBridgeDanmaku : ILiveDanmaku
    {
        public event EventHandler<LiveMessage> NewMessage;
        public event EventHandler<string> OnClose;
        public int HeartbeatTime => 0;
        private ClientWebSocket socket;
        private CancellationTokenSource lifetime;
        private Task receiveTask;
        public void Heartbeat() { }
        public async Task Start(object args)
        {
            await Stop();
            var current = new ClientWebSocket();
            var cancellation = new CancellationTokenSource();
            socket = current; lifetime = cancellation;
            try
            {
                using (var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellation.Token))
                {
                    timeout.CancelAfter(10000);
                    await current.ConnectAsync(new Uri("ws://127.0.0.1:17865/danmaku?session=" + Uri.EscapeDataString(args.ToString())), timeout.Token).ConfigureAwait(false);
                }
                receiveTask = Receive(current, cancellation.Token);
            }
            catch { await Stop(); throw; }
        }
        private async Task Receive(ClientWebSocket current, CancellationToken token)
        {
            var buffer = new byte[4096];
            try
            {
                while (!token.IsCancellationRequested && current.State == WebSocketState.Open)
                {
                    using (var message = new MemoryStream())
                    {
                        WebSocketReceiveResult result;
                        do
                        {
                            result = await current.ReceiveAsync(new ArraySegment<byte>(buffer), token).ConfigureAwait(false);
                            if (result.MessageType == WebSocketMessageType.Close)
                            { if (!token.IsCancellationRequested) OnClose?.Invoke(this, "本地弹幕连接已关闭"); return; }
                            if (message.Length + result.Count > 1024 * 1024) throw new InvalidDataException("Danmaku message too large");
                            message.Write(buffer, 0, result.Count);
                        } while (!result.EndOfMessage);
                        if (result.MessageType != WebSocketMessageType.Text) continue;
                        LiveMessage parsed;
                        try
                        {
                            var m = JObject.Parse(Encoding.UTF8.GetString(message.ToArray()));
                            parsed = new LiveMessage {
                                Type = (LiveMessageType)m["Type"].Value<int>(), UserName = m["UserName"].ToString(),
                                Message = m["Message"].ToString(), Data = m["Data"]?.Value<long?>() ?? 0,
                                Color = new DanmakuColor(m["Color"].ToString())
                            };
                        }
                        catch (Exception) { continue; }
                        NewMessage?.Invoke(this, parsed);
                    }
                }
            }
            catch (OperationCanceledException) { }
            catch (Exception ex) { if (!token.IsCancellationRequested) OnClose?.Invoke(this, "弹幕接收失败：" + ex.GetType().Name); }
        }
        public async Task Stop()
        {
            var oldSocket = socket; var oldCancellation = lifetime; var oldReceive = receiveTask;
            socket = null; lifetime = null; receiveTask = null;
            oldCancellation?.Cancel(); oldSocket?.Abort();
            if (oldReceive != null) await oldReceive.ConfigureAwait(false);
            oldSocket?.Dispose(); oldCancellation?.Dispose();
        }
    }
}
