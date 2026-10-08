using AllLive.Core.Helper;
using AllLive.Core.Interface;
using AllLive.Core.Models;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading.Tasks;

namespace AllLive.Core
{
    // Only Douyu uses the pinned Simple Live core; playback stays in the official player.
    public sealed class Douyu : ILiveSite
    {
        internal const string BaseUrl = "http://127.0.0.1:17865";
        private static readonly HttpClient Client = new HttpClient { Timeout = TimeSpan.FromSeconds(75) };
        private const string id = "douyu";
        public string Name => "斗鱼直播";
        public static Func<Task> EnsureStarted { get; set; }

        private async Task<JToken> Call(string method, object args = null)
        {
            if (EnsureStarted != null) await EnsureStarted();
            var data = args == null ? new JObject() : JObject.FromObject(args);
            var body = JsonConvert.SerializeObject(new { site = id, method, args = data });
            using (var response = await Client.PostAsync(BaseUrl + "/rpc", new StringContent(body, System.Text.Encoding.UTF8, "application/json")))
            {
                var json = JObject.Parse(await response.Content.ReadAsStringAsync());
                if (!response.IsSuccessStatusCode) throw new InvalidOperationException(json["error"]?.ToString() ?? "Douyu helper unavailable");
                return json["result"];
            }
        }
        public ILiveDanmaku GetDanmaku() => new Danmaku.DouyuBridgeDanmaku();
        public async Task<List<LiveCategory>> GetCategores() => (await Call("categories")).ToObject<List<LiveCategory>>();
        public async Task<LiveCategoryResult> GetRecommendRooms(int page = 1) => (await Call("recommend", new { page })).ToObject<LiveCategoryResult>();
        public async Task<LiveCategoryResult> GetCategoryRooms(LiveSubCategory category, int page = 1) => (await Call("categoryRooms", new { category, page })).ToObject<LiveCategoryResult>();
        public async Task<LiveSearchResult> Search(string keyword, int page = 1) => (await Call("search", new { keyword, page })).ToObject<LiveSearchResult>();
        public async Task<LiveRoomDetail> GetRoomDetail(object roomId) => (await Call("detail", new { roomId = roomId.ToString() })).ToObject<LiveRoomDetail>();
        public async Task<List<LivePlayQuality>> GetPlayQuality(LiveRoomDetail roomDetail) => (await Call("qualities", new { session = roomDetail.Data.ToString() })).ToObject<List<LivePlayQuality>>();
        public async Task<List<string>> GetPlayUrls(LiveRoomDetail roomDetail, LivePlayQuality qn)
        {
            var result = await Call("urls", new { session = roomDetail.Data.ToString(), quality = Convert.ToInt32(qn.Data) });
            return result["urls"].ToObject<List<string>>();
        }
        public async Task<bool> GetLiveStatus(object roomId) => (await Call("status", new { roomId = roomId.ToString() })).Value<bool>();
        public async Task<List<LiveSuperChatMessage>> GetSuperChatMessages(object roomId) => (await Call("superChat", new { roomId = roomId.ToString() })).ToObject<List<LiveSuperChatMessage>>();
    }
}
