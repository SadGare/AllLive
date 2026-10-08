using System;
using System.Collections.Generic;
using System.Linq;

namespace AllLive.UWP.Helper
{
    public static class NavigationOrder
    {
        private const string Key = "NavigationTabOrder";
        public static readonly string[] Default = { "FavoritePage", "RecomendPage", "CategoryPage", "HistoryPage", "SyncPage" };
        public static string[] Load()
        {
            var saved = SettingHelper.GetValue<string>(Key, string.Join(",", Default));
            return Normalize(saved.Split(','));
        }
        public static string[] Normalize(IEnumerable<string> ids) =>
            ids.Where(x => Default.Contains(x)).Concat(Default).Distinct().ToArray();
        public static void Save(IEnumerable<string> ids)
        {
            SettingHelper.SetValue(Key, string.Join(",", Normalize(ids)));
            MessageCenter.UpdatePanelDisplayMode();
        }
    }
    public sealed class NavigationTabOption
    {
        public string Id { get; set; }
        public string Label { get; set; }
    }
}
