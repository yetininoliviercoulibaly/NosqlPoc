using Couchbase;
using Couchbase.Authentication;
using Couchbase.Core;
using Couchbase.Core.Serialization;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CouchbaseTest
{
    class Program
    {
        static void Main(string[] args)
        {
            var product = GetFakeProduct();
            var config = new Couchbase.Configuration.Client.ClientConfiguration
            {
                Servers = new List<Uri>() { new Uri("http://40.114.146.85:8091") },
                Serializer = () => new DefaultSerializer(),

            };

            config.SetAuthenticator(new PasswordAuthenticator("test", "test1234"));
            ClusterHelper.Initialize(config);

            var bucket = ClusterHelper.GetBucket("Catalog");
            var key = $"{product.Type}::{product.Id}"; 
            bucket.Upsert(key, product);

            var paths = GetPaths("fr-FR", product);

            var p = GetFragmentProduct(bucket, key, paths);

            Console.ReadLine(); 

        }

        private static Product GetFragmentProduct(IBucket bucket, string key, IEnumerable<string> paths)
        {
            var builder = bucket.LookupIn<dynamic>(key);
            foreach (var path in paths)
            {
                builder = builder.Get(path);
            }
            var doc = builder.Execute();
            var result = new Product();
           

            result.Id = doc.Content<string>("id");
            result.Type = doc.Content<string>("type");
            result.ProductId = doc.Content<int>("productId");
            result.MarketInfo = new Dictionary<string, MarketInfo>();
            result.MarketInfo["fr"] = doc.Content<MarketInfo>("marketInfo.fr"); 
            
            return result;

        }

        private static Product GetFakeProduct()
        {
            var product = new Product()
            {
                Id = "1::123",
                ProductId = 123,
                Type = "Article"
            };

            product.LanguageInfo = new Dictionary<string, LanguageInfo>();
            product.LanguageInfo["fr-FR"] = new LanguageInfo
            {
                SubTitle = "sub title fr-FR",
                Title = "Title fr-FR"
            };

            product.LanguageInfo["fr-BE"] = new LanguageInfo
            {
                SubTitle = "sub title fr-BE",
                Title = "Title fr-BE"
            };


            product.LanguageInfo["nl-BE"] = new LanguageInfo
            {
                SubTitle = "sub title nl-BE",
                Title = "Title nl-BE"
            };


            product.MarketInfo = new Dictionary<string, MarketInfo>();
            product.MarketInfo["fr"] = new MarketInfo
            {
                Availability = 199,
                Price = 200
            };

            product.MarketInfo["be"] = new MarketInfo
            {
                Availability = 101,
                Price = 198
            };

            return product;
        }

        private static IEnumerable<string> GetPaths(string culture, Product p)
        {

            var result = new List<string>();
            var market = culture.Split('-')[1]; 
            var properties = p.GetType().GetProperties();
            var framentProperties = properties.Where(o => o.CustomAttributes.Any(a => a.AttributeType == typeof(FragmentPropertyAttribute)));
            var jsonAttributes = properties.Where(o => !framentProperties.Contains(o)).Select(o => o.GetCustomAttributes(false).FirstOrDefault(a => a.GetType() == typeof(JsonPropertyAttribute))).Cast<JsonPropertyAttribute>();
            result.AddRange(jsonAttributes.Select(o => o.PropertyName)); 
           
            foreach(var fragmentProperty in framentProperties)
            {
                var attribute = p.GetType().GetProperty(fragmentProperty.Name).GetCustomAttributes(false).FirstOrDefault(o => o.GetType() == typeof(FragmentPropertyAttribute)) as FragmentPropertyAttribute;
                result.Add(attribute.Path.Replace("{market}", market.ToLower()).Replace("{culture}",culture));
            }
            return result;

        }
    }


    public class FragmentPropertyAttribute : Attribute
    {
        public string Path { get; set; }

        public FragmentPropertyAttribute(string path)
        {
            Path = path;
        }
    }

    public class Product
    {

        [JsonProperty("id")]
        public string Id { get; set; }

        [JsonProperty("type")]
        public string Type { get; set; }

        [JsonProperty("productId")]
        public int ProductId { get; set; }

        [FragmentProperty("marketInfo.{market}")]
        public IDictionary<string, MarketInfo> MarketInfo { get; set; }


        [FragmentProperty("languageInfo.{culture}")]
        public IDictionary<string, LanguageInfo> LanguageInfo { get; set; }

    }

    public class LanguageInfo
    {
        public string Title { get; set; }
        public string SubTitle { get; set; }

    }

    public class MarketInfo
    {
        public decimal Price { get; set; }
        public int Availability { get; set; }
    }
}
