using AllLive.Core;
using Newtonsoft.Json.Linq;
using System;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Windows.ApplicationModel;

namespace AllLive.UWP.Helper
{
    public static class DouyuBridge
    {
        private static readonly SemaphoreSlim Gate = new SemaphoreSlim(1, 1);
        private static readonly HttpClient Client = new HttpClient { Timeout = TimeSpan.FromSeconds(2) };
        private static DateTime checkedAt = DateTime.MinValue;
        public static async Task EnsureStarted()
        {
            if ((DateTime.UtcNow - checkedAt).TotalSeconds < 30) return;
            await Gate.WaitAsync();
            try
            {
                if ((DateTime.UtcNow - checkedAt).TotalSeconds < 30) return;
                if (!await IsReady())
                {
                    await FullTrustProcessLauncher.LaunchFullTrustProcessForCurrentAppAsync();
                    for (int i = 0; i < 40 && !await IsReady(); i++) await Task.Delay(250);
                    if (!await IsReady()) throw new InvalidOperationException("本地 Simple Live 核心启动失败，请运行安装包中的 Install.ps1 检查网络回环权限。");
                }
                checkedAt = DateTime.UtcNow;
            }
            finally { Gate.Release(); }
        }
        private static async Task<bool> IsReady()
        {
            try
            {
                var result = JObject.Parse(await Client.GetStringAsync("http://127.0.0.1:17865/health"));
                return result["protocol"]?.Value<int>() == 1 && result["scope"]?.ToString() == "douyu" &&
                    result["coreRevision"]?.ToString() == "ba828e6783b176ea5709fcd09f0eb01dfaceeb51";
            }
            catch { return false; }
        }
        public static void Configure()
        {
            Douyu.EnsureStarted = EnsureStarted;
        }
    }
}
