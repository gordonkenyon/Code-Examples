using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Net;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using SageAPI.Models;
using Sage300DLL;
using Newtonsoft.Json;
using MyFreightDLLStandard;
using NLog;
using Contracts;
using System.Diagnostics;
using System.Text;
using MyFreightAPI;

//using MyFreightAPI;
//using MyFreightDLLStandard;

namespace SageAPI.V2.Controllers
{
    //  [ApiVersion("1", Deprecated = true)]
    [ApiVersion("3")]
    [Authorize]
    [Route("v{version:apiVersion}/api/[controller]")]
    [ApiController]
    public class SagePricingController : ControllerBase
    {
       

        private readonly ILoggerManager _logger;

        private readonly IConfiguration _configuration;


        public SagePricingController(IConfiguration configuration,ILoggerManager logger)
        {
            _configuration = configuration;
            _logger = logger;
        }
       
        public static string ItemsToJson( Sage300DLL.BareQuoteItems value)
        {
            string detail = "";
                        
            detail = detail + JsonConvert.SerializeObject(value);
            

            var settings = new JsonSerializerSettings
            {
                ReferenceLoopHandling = ReferenceLoopHandling.Ignore
            };

            return JsonConvert.SerializeObject(value, Formatting.Indented, settings);
        }


        public static string QuoteItemsToJson(List<Sage300DLL.QuoteItem> value)
        {
            string detail = "";

            detail = detail + JsonConvert.SerializeObject(value);


            var settings = new JsonSerializerSettings
            {
                ReferenceLoopHandling = ReferenceLoopHandling.Ignore
            };

            return JsonConvert.SerializeObject(value, Formatting.Indented, settings);
        }

        public static string CarriersResponseToJson(System.Collections.Generic.ICollection<MyFreightAPI.PricingQuoteResults_carriers> value)
        {
            string detail = "";

            detail = detail + JsonConvert.SerializeObject(value);


            var settings = new JsonSerializerSettings
            {
                ReferenceLoopHandling = ReferenceLoopHandling.Ignore
            };

            return JsonConvert.SerializeObject(value, Formatting.Indented, settings);
        }

        [ApiVersion("3")]
        [HttpPut("FreightQuote")]
        public List<Quotes> GetItemQuotes([FromBody] Sage300DLL.BareQuoteItems Items)
        {
            

            string detail = ItemsToJson(Items);

            EventLog myLog = new EventLog();
            myLog.Source = "StarSageAPI";
           
            myLog.WriteEntry("Freight Quote" + detail);
           

           

            //lookup Sage for the SITE code and address based on CLIENTSTATE
            string CS = _configuration.GetConnectionString("PricingConnection").ToString();
            string[] parameters = new string[0];
            Sage300DLL.OrderEntry orderEntry = new Sage300DLL.OrderEntry(parameters);

            string SageDatabase = _configuration.GetConnectionString("SAGE_Database").ToString();
            string SageVersion = _configuration.GetConnectionString("SAGE_Version").ToString();
            string User = _configuration.GetConnectionString("SAGE_User").ToString();
            string Password = _configuration.GetConnectionString("SAGE_Password").ToString();

           

            List<Sage300DLL.QuoteItem> quoteItems = new List<Sage300DLL.QuoteItem>();
            

            quoteItems = orderEntry.GetItemDetailsFromBareWebItems(Items, SageDatabase, SageVersion, User, Password);

            if (quoteItems.Count == 0 || quoteItems.Count != Items.Items.Count)
            {
                //detail = "No item details are available in Sage for the items selected!";
                detail = ItemsToJson(Items);
                byte[] verror = Encoding.ASCII.GetBytes(detail);
                myLog.WriteEntry("Invalid Quote Items" + detail, EventLogEntryType.Error, 0, 0, verror);             

                throw new NotFoundException($"ItemNo's - All Weight and Freight Dimensions for all items were not found! - {detail}");
            }
            else
            {

                detail = QuoteItemsToJson(quoteItems);
             

                byte[] vok = Encoding.ASCII.GetBytes(detail);
                myLog.WriteEntry("Quote Items" + detail, EventLogEntryType.Information, 0, 0, vok);
            }

            MyFreightDLLStandard.QuoteRequestFromWeb quoteRequestFromWeb = new MyFreightDLLStandard.QuoteRequestFromWeb();


            for (int i = 0; i < quoteItems.Count; i++)
            {
                MyFreightDLLStandard.QuoteItem item = new MyFreightDLLStandard.QuoteItem();

                item.Item_type_Description = "Carton";
                //TODO when changed from Starleaton to Pallet etc.. needs to be setup in Sage first 
                item.Height = Convert.ToInt32(quoteItems[i].HEIGHT);
                item.Length = Convert.ToInt32(quoteItems[i].LENGTH);
                item.Width = Convert.ToInt32(quoteItems[i].WIDTH);
                item.Weight_in_Kilograms = Convert.ToString(quoteItems[i].UNITWGT);
                item.Quantity = Convert.ToInt32(quoteItems[i].QTY);
                item.Reference = Items.Reference;

                quoteRequestFromWeb.Items.Add(item);
            }
           
            // Find out the Sender details 
            MyFreightDLLStandard.QuoteReceiverAddress quoteReceiverAddress = new MyFreightDLLStandard.QuoteReceiverAddress();

            quoteReceiverAddress.Country = Items.ReceiverCountry;
            quoteReceiverAddress.Locality = Items.RecieverCity;
            quoteReceiverAddress.PostCode = Items.ReceiverPostCode;
            quoteReceiverAddress.Region = Items.RecieverState;

            quoteRequestFromWeb.Receiver_address = quoteReceiverAddress;

            quoteRequestFromWeb.Special_instructions = "";
            quoteRequestFromWeb.Despatch_date = DateTime.Now;

            MyFreight myFreight = new MyFreight();
            System.Collections.Generic.ICollection<MyFreightAPI.PricingQuoteResults_carriers> pricingQuoteResults = myFreight.ObtainQuotes(quoteRequestFromWeb);

            detail = CarriersResponseToJson(pricingQuoteResults);

            if (pricingQuoteResults.Count == 0)
            {
                byte[] vs = Encoding.ASCII.GetBytes(detail);
                myLog.WriteEntry("No Quote returned from MyFreight" + detail, EventLogEntryType.Error, 0, 0, vs);

                throw new NotFoundException($"No MyFreight quote was received , check the Receiver City and Receiver PostCode - {detail}");
            }
            else
            {
                //_logger.LogInfo("Carriers Quote" + detail);

                byte[] vs = Encoding.ASCII.GetBytes(detail);
                myLog.WriteEntry("Carriers Quote" + detail, EventLogEntryType.Information, 0, 0, vs);
            }

           

            decimal quotePercentage = Convert.ToDecimal(_configuration["QuotePercentage"]);


            string CarriersNotAllowed = _configuration["CarriersNotAllowedOnWeb"].ToString();

            string[] CarriersNotAllowedOnWeb = CarriersNotAllowed.Split(',');




            List<Quotes> quotes = new List<Quotes>();
            foreach (var carrier in pricingQuoteResults)
            {
                //Check for a valid carrier to return to web 
           
                    bool CarrierNotAllowed = CarriersNotAllowedOnWeb.Contains(carrier.Carrier);


                if (CarrierNotAllowed == false)
                { 

                        Quotes quote = new Quotes();
                        quote.CARRIER = carrier.Carrier.ToString();
                        quote.SERVICE = carrier.Service.ToString();
                        quote.SERVICECODE = carrier.Service_code.ToString();
                        quote.TOTAL = Convert.ToString((Convert.ToDecimal(carrier.Total.ToString())) + (Convert.ToDecimal(carrier.Total.ToString()) / quotePercentage));
                        quote.TOTALEX = Convert.ToString(Convert.ToDecimal(carrier.Total_exclusive.ToString()) + (Convert.ToDecimal(carrier.Total_exclusive.ToString()) / quotePercentage));
                        quote.ESTIMATEDDELIVERYDATE = carrier.Estimated_delivery_date.ToString();

                        quotes.Add(quote);
                       
                }

            }

            return quotes;
        }




        




    }
}
