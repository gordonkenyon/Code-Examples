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
       

        [ApiVersion("3")]
        [HttpGet("")]
        public List<ItemsStatus> Get()
        {
            List<ItemsStatus> ItemsStatus = new List<ItemsStatus>();

            string CS = _configuration.GetConnectionString("PricingConnection").ToString();

            //  List<SageCustomer> customers = new List<SageCustomer>();


            using (SqlConnection con2 = new SqlConnection(CS))
            {


                con2.Open();

                // Check if item is active and is allowed on web




                SqlCommand cmdGetIsValid = new SqlCommand("SELECT ITEMNO, INACTIVE, ALLOWONWEB FROM ICITEM ", con2);

                cmdGetIsValid.CommandType = CommandType.Text;


                SqlDataReader rdrIsValid = cmdGetIsValid.ExecuteReader();



                while (rdrIsValid.Read())
                {
                    ItemsStatus itemStatus = new ItemsStatus();
                    itemStatus.ITEMNO = rdrIsValid["ITEMNO"].ToString();
                    itemStatus.INACTIVE = rdrIsValid["INACTIVE"].ToString();
                    itemStatus.ALLOWONWEB = rdrIsValid["ALLOWONWEB"].ToString();
                    ItemsStatus.Add(itemStatus);
                }

                cmdGetIsValid.Dispose();
                rdrIsValid.Close();


            }

            return ItemsStatus.ToList();

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


        [ApiVersion("3")]
        [HttpGet("{IDCUST},{ITEMNO}")]
        public List<ItemPriceByCustomer> Get(string IDCUST, string ITEMNO)
        {
            List<ItemPriceByCustomer> prices = new List<ItemPriceByCustomer>();

            string CS = _configuration.GetConnectionString("PricingConnection").ToString();

            //  List<SageCustomer> customers = new List<SageCustomer>();


            using (SqlConnection con = new SqlConnection(CS))
            {

                Boolean isActiveAndAllowedOnWeb = false;

                con.Open();

                // Check if item is active and is allowed on web




                SqlCommand cmdGetIsValid = new SqlCommand("SELECT ITEMNO, INACTIVE, ALLOWONWEB FROM ICITEM WHERE(ITEMNO = '" + ITEMNO + "') AND(INACTIVE = 0) AND(ALLOWONWEB = 1)", con);

                cmdGetIsValid.CommandType = CommandType.Text;


                SqlDataReader rdrIsValid = cmdGetIsValid.ExecuteReader();

                if ((rdrIsValid.HasRows) == false)
                {
                    isActiveAndAllowedOnWeb = false;
                }
                else
                {
                    isActiveAndAllowedOnWeb = true;
                }

                cmdGetIsValid.Dispose();
                rdrIsValid.Close();

                // get IDCUST price level 

                SqlCommand cmdGetPriceLevel = new SqlCommand("SELECT IDCUST, PRICLIST, CUSTTYPE, DATEADD(mi, DATEDIFF(mi, GETUTCDATE(), GETDATE()),CONVERT(VARCHAR(10), CONVERT(Date, CONVERT(VARCHAR(8), AUDTDATE))) + ' ' +LEFT(LEFT(Right('00000000' + Convert(varchar(8), AUDTTIME), 8), 4), 2) + ':' +RIGHT(LEFT(right('00000000' + Convert(varchar(8), AUDTTIME), 8), 4), 2)+ ':' + RIGHT(LEFT(right('00000000' + Convert(varchar(8), AUDTTIME), 8), 6), 2)) AS AUDTDATE FROM ARCUS WHERE IDCUST = '" + IDCUST + "'", con);
                cmdGetPriceLevel.CommandType = CommandType.Text;
                // con.Open();

                SqlDataReader rdrPriceLevel = cmdGetPriceLevel.ExecuteReader();

                var customerType = new SageCustomerPriceListType();
                while (rdrPriceLevel.Read())
                {
                    customerType.IDCUST = IDCUST;
                    customerType.PRICLIST = rdrPriceLevel["PRICLIST"].ToString();
                    customerType.CUSTTYPE = rdrPriceLevel["CUSTTYPE"].ToString();
                    customerType.CUSTOMER_AUDTDATE = (DateTime)rdrPriceLevel["AUDTDATE"]; //, "yyyyMMdd", null); // rdrPriceLevel["AUDTDATE"].ToString();
                    //customerType.CUSTOMER_AUDTTIME = rdrPriceLevel["AUDTTIME"].ToString();//, "hh:mm", null); //rdrPriceLevel["AUDTDATE"].ToString(); rdrPriceLevel["AUDTTIME"].ToString();
                }
                //con.Close();

                cmdGetPriceLevel.Dispose();
                rdrPriceLevel.Close();

                var pricePossibility = new Sage300DLL.SagePricePossibility();

                SqlCommand cmdGetAUDTDATE = new SqlCommand("SELECT DATEADD(mi, DATEDIFF(mi, GETUTCDATE(), GETDATE()),CONVERT(VARCHAR(10), CONVERT(Date, CONVERT(VARCHAR(8), AUDTDATE))) + ' ' +LEFT(LEFT(Right('00000000' + Convert(varchar(8), AUDTTIME), 8), 4), 2) + ':' +RIGHT(LEFT(right('00000000' + Convert(varchar(8), AUDTTIME), 8), 4), 2)+ ':' + RIGHT(LEFT(right('00000000' + Convert(varchar(8), AUDTTIME), 8), 6), 2)) AS AUDTDATE FROM ICPRIC WHERE PRICELIST = 'WEB' AND ITEMNO = '" + ITEMNO + "'", con);
                SqlDataReader rdrAUDDATE = cmdGetAUDTDATE.ExecuteReader();
                while (rdrAUDDATE.Read())
                {
                    pricePossibility.BASEPRICE_AUDTDATE = (DateTime)rdrAUDDATE["AUDTDATE"]; //, "yyyyMMdd", null);
                }
                cmdGetAUDTDATE.Dispose();
                rdrAUDDATE.Close();

                // customerType now contains all we need to move to next step

                SqlCommand cmdGetPricePossibility1 = new SqlCommand("SELECT ICPRICP.ITEMNO, ICPRICP.PRICELIST, ICPRICP.DPRICETYPE," +
                    " ICPRICP.UNITPRICE, ICPRICP.SALESTART, ICPRICP.SALEEND, ICPRIC.PRICETYPE, ICPRIC.PRCNTLVL1,ICPRIC.PRCNTLVL2," +
                    " ICPRIC.PRCNTLVL3, ICPRIC.PRCNTLVL4, ICPRIC.PRCNTLVL5, ARCUS.IDCUST, ARCUS.TEXTSTRE2, ARCUS.PRICLIST," +
                    " ARCUS.CUSTTYPE,ICITEM.CATEGORY , DATEADD(mi, DATEDIFF(mi, GETUTCDATE(), GETDATE()),CONVERT(VARCHAR(10), CONVERT(Date, CONVERT(VARCHAR(8), ICPRICP.AUDTDATE))) + ' ' +LEFT(LEFT(Right('00000000' + Convert(varchar(8), ICPRICP.AUDTTIME), 8), 4), 2) + ':' +RIGHT(LEFT(right('00000000' + Convert(varchar(8), ICPRICP.AUDTTIME), 8), 4), 2)+ ':' + RIGHT(LEFT(right('00000000' + Convert(varchar(8), ICPRICP.AUDTTIME), 8), 6), 2)) AS AUDTDATE FROM ICPRICP INNER JOIN ICPRIC ON ICPRICP.ITEMNO = ICPRIC.ITEMNO AND ICPRICP.PRICELIST = ICPRIC.PRICELIST" +
                    " INNER JOIN ARCUS ON ICPRICP.PRICELIST = ARCUS.PRICLIST INNER JOIN " +
                    " ICITEM ON ICPRICP.ITEMNO = ICITEM.ITEMNO" +
                    " WHERE (ICPRICP.ITEMNO = '" + ITEMNO + "') " +
                    " AND (ARCUS.IDCUST = '" + IDCUST + "') AND (ICPRICP.DPRICETYPE = 1)", con);

                SqlDataReader rdrPricePossibility1 = cmdGetPricePossibility1.ExecuteReader();



                while (rdrPricePossibility1.Read())
                {

                    pricePossibility.IDCUST = IDCUST;
                    pricePossibility.ITEMNO = ITEMNO;
                    pricePossibility.PRICETYPE = 1;
                    pricePossibility.PRICLIST = rdrPricePossibility1["PRICLIST"].ToString();
                    pricePossibility.UNITPRICE = Convert.ToDecimal(rdrPricePossibility1["UNITPRICE"].ToString());
                    if (rdrPricePossibility1["SALESTART"].ToString() == "0")
                    {
                        pricePossibility.SALESTART = Convert.ToDateTime("01/01/2000");
                    }
                    else
                    {
                        pricePossibility.SALESTART = DateTime.ParseExact(rdrPricePossibility1["SALESTART"].ToString(), "yyyyMMdd", null);
                    }
                    if (rdrPricePossibility1["SALEEND"].ToString() == "0")
                    {
                        pricePossibility.SALEEND = Convert.ToDateTime("01/01/2999");
                    }
                    else
                    {
                        pricePossibility.SALEEND = DateTime.ParseExact(rdrPricePossibility1["SALEEND"].ToString(), "yyyyMMdd", null);
                    }

                    pricePossibility.PRCNTLVL1 = Convert.ToDecimal(rdrPricePossibility1["PRCNTLVL1"].ToString());
                    pricePossibility.PRCNTLVL2 = Convert.ToDecimal(rdrPricePossibility1["PRCNTLVL2"].ToString());
                    pricePossibility.PRCNTLVL3 = Convert.ToDecimal(rdrPricePossibility1["PRCNTLVL3"].ToString());
                    pricePossibility.PRCNTLVL4 = Convert.ToDecimal(rdrPricePossibility1["PRCNTLVL4"].ToString());
                    pricePossibility.PRCNTLVL5 = Convert.ToDecimal(rdrPricePossibility1["PRCNTLVL5"].ToString());

                    pricePossibility.CUSTTYPE = Convert.ToInt32(rdrPricePossibility1["CUSTTYPE"].ToString());

                    pricePossibility.CATEGORY = rdrPricePossibility1["CATEGORY"].ToString();

                    pricePossibility.PP1_AUDTDATE = (DateTime)rdrPricePossibility1["AUDTDATE"];

                }


                // Apply PricePossibility1 business rule 1

                //                Price would be UNITPRICE *(1 - CUSTOMERLEVEL)
                //IF CUSTTYPE = 0 then 0 else
                //                    IF CUSTTYPE = 1 then CUSTOMERLEVEL = PRCNTLVL1
                //IF CUSTTYPE = 2 then CUSTOMERLEVEL = PRCNTLVL2
                //IF CUSTTYPE = 3 then CUSTOMERLEVEL = PRCNTLVL3
                //IF CUSTTYPE = 4 then CUSTOMERLEVEL = PRCNTLVL4
                //IF CUSTTYPE = 5 then CUSTOMERLEVEL = PRCNTLVL5

                switch (pricePossibility.CUSTTYPE)
                {
                    case 0:
                        pricePossibility.PRICEPOSSIBILITY1 = pricePossibility.UNITPRICE;
                        break;
                    case 1:
                        pricePossibility.PRICEPOSSIBILITY1 = pricePossibility.UNITPRICE * (1 - pricePossibility.PRCNTLVL1 / 100);
                        break;
                    case 2:
                        pricePossibility.PRICEPOSSIBILITY1 = pricePossibility.UNITPRICE * (1 - pricePossibility.PRCNTLVL2 / 100);
                        break;
                    case 3:
                        pricePossibility.PRICEPOSSIBILITY1 = pricePossibility.UNITPRICE * (1 - pricePossibility.PRCNTLVL3 / 100);
                        break;
                    case 4:
                        pricePossibility.PRICEPOSSIBILITY1 = pricePossibility.UNITPRICE * (1 - pricePossibility.PRCNTLVL4 / 100);
                        break;
                    case 5:
                        pricePossibility.PRICEPOSSIBILITY1 = pricePossibility.UNITPRICE * (1 - pricePossibility.PRCNTLVL5 / 100);
                        break;
                    default:
                        break;
                }


                cmdGetPricePossibility1.Dispose();
                rdrPricePossibility1.Close();

                SqlCommand cmdGetPricePossibility2 = new SqlCommand("SELECT ICPRICP.ITEMNO, ICPRICP.PRICELIST, ICPRICP.DPRICETYPE, " +
                    " ICPRICP.UNITPRICE, ICPRICP.SALESTART, ICPRICP.SALEEND, ICPRIC.PRICETYPE, ICPRIC.PRCNTLVL1,ICPRIC.PRCNTLVL2," +
                    " ICPRIC.PRCNTLVL3, ICPRIC.PRCNTLVL4, ICPRIC.PRCNTLVL5, ARCUS.IDCUST, ARCUS.TEXTSTRE2, ARCUS.PRICLIST," +
                    " ARCUS.CUSTTYPE, DATEADD(mi, DATEDIFF(mi, GETUTCDATE(), GETDATE()),CONVERT(VARCHAR(10), CONVERT(Date, CONVERT(VARCHAR(8), ICPRICP.AUDTDATE))) + ' ' +LEFT(LEFT(Right('00000000' + Convert(varchar(8), ICPRICP.AUDTTIME), 8), 4), 2) + ':' +RIGHT(LEFT(right('00000000' + Convert(varchar(8), ICPRICP.AUDTTIME), 8), 4), 2)+ ':' + RIGHT(LEFT(right('00000000' + Convert(varchar(8), ICPRICP.AUDTTIME), 8), 6), 2)) AS AUDTDATE FROM ICPRICP INNER JOIN ICPRIC ON ICPRICP.ITEMNO = ICPRIC.ITEMNO AND ICPRICP.PRICELIST = ICPRIC.PRICELIST" +
                    " INNER JOIN ARCUS ON ICPRICP.PRICELIST = ARCUS.PRICLIST WHERE (ICPRICP.ITEMNO = '" + ITEMNO + "') " +
                    " AND (ARCUS.IDCUST = '" + IDCUST + "') AND (ICPRICP.DPRICETYPE = 2)", con);

                SqlDataReader rdrPricePossibility2 = cmdGetPricePossibility2.ExecuteReader();

                while (rdrPricePossibility2.Read())
                {

                    pricePossibility.PRICEPOSSIBILITY2 = Convert.ToDecimal(rdrPricePossibility2["UNITPRICE"].ToString());

                    if (rdrPricePossibility2["SALESTART"].ToString() == "0")
                    {
                        pricePossibility.SALESTART = Convert.ToDateTime("01/01/2000");
                    }
                    else
                    {
                        pricePossibility.SALESTART = DateTime.ParseExact(rdrPricePossibility2["SALESTART"].ToString(), "yyyyMMdd", null);
                    }
                    if (rdrPricePossibility2["SALEEND"].ToString() == "0")
                    {
                        pricePossibility.SALEEND = Convert.ToDateTime("01/01/2000");
                    }
                    else
                    {
                        pricePossibility.SALEEND = DateTime.ParseExact(rdrPricePossibility2["SALEEND"].ToString(), "yyyyMMdd", null);
                    }

                    pricePossibility.PP2_AUDTDATE =  (DateTime)rdrPricePossibility2["AUDTDATE"];

                }



                cmdGetPricePossibility2.Dispose();
                rdrPricePossibility2.Close();


                SqlCommand cmdGetPricePossibility3 = new SqlCommand("SELECT ARCUS.IDCUST, ICCUPR.PRICEBY, ICCUPR.CATEGORY, ICCUPR.ITEMNO," +
                    " ICCUPR.PRICELIST, ICCUPR.PRICETYPE, ICCUPR.DISCPER, ICCUPR.FIXPRICE,ICCUPR.STARTDATE, ICCUPR.USELOWEST, ICCUPR.EXPIRE, DATEADD(mi, DATEDIFF(mi, GETUTCDATE(), GETDATE()),CONVERT(VARCHAR(10), CONVERT(Date, CONVERT(VARCHAR(8), ICCUPR.AUDTDATE))) + ' ' +LEFT(LEFT(Right('00000000' + Convert(varchar(8), ICCUPR.AUDTTIME), 8), 4), 2) + ':' +RIGHT(LEFT(right('00000000' + Convert(varchar(8), ICCUPR.AUDTTIME), 8), 4), 2)+ ':' + RIGHT(LEFT(right('00000000' + Convert(varchar(8), ICCUPR.AUDTTIME), 8), 6), 2)) AS AUDTDATE FROM ARCUS" +
                    " INNER JOIN ICCUPR ON ARCUS.IDCUST = ICCUPR.CUSTNO WHERE (ARCUS.IDCUST = '" + IDCUST + "') AND ICCUPR.ITEMNO = '" + pricePossibility.ITEMNO + "' AND (ICCUPR.PRICEBY = 2)", con);

                SqlDataReader rdrPricePossibility3 = cmdGetPricePossibility3.ExecuteReader();

                while (rdrPricePossibility3.Read())
                {
                    if (rdrPricePossibility3["USELOWEST"].ToString() == "0")
                    {

                        pricePossibility.USELOWESTPRICE = false;
                    }
                    else
                    {
                        pricePossibility.USELOWESTPRICE = true;
                    }
                    pricePossibility.PRICETYPE = Convert.ToInt32(rdrPricePossibility3["PRICETYPE"].ToString());
                    pricePossibility.DISCPER = Convert.ToDecimal(rdrPricePossibility3["DISCPER"].ToString());
                    pricePossibility.FIXPRICE = Convert.ToDecimal(rdrPricePossibility3["FIXPRICE"].ToString());
                    if (rdrPricePossibility3["STARTDATE"].ToString() == "0")
                    {
                        pricePossibility.STARTDATE = Convert.ToDateTime("01/01/2000");
                    }
                    else
                    {
                        pricePossibility.STARTDATE = DateTime.ParseExact(rdrPricePossibility3["STARTDATE"].ToString(), "yyyyMMdd", null);
                    }

                    if (rdrPricePossibility3["EXPIRE"].ToString() == "0")
                    {
                        pricePossibility.EXPIRE = Convert.ToDateTime("01/01/2999");
                    }
                    else
                    {
                        pricePossibility.EXPIRE = DateTime.ParseExact(rdrPricePossibility3["EXPIRE"].ToString(), "yyyyMMdd", null);
                    }



                    switch (pricePossibility.PRICETYPE)
                    {
                        case 0:
                            break;
                        case 1:
                            // Customer Type
                            break;
                        case 2:
                            // Discount Percentage
                            if (pricePossibility.EXPIRE <= DateTime.Today)
                            {

                            }
                            else
                            {
                                pricePossibility.PRICEPOSSIBILITY3 = pricePossibility.UNITPRICE - (pricePossibility.DISCPER / 100 * pricePossibility.UNITPRICE);
                            }
                            break;
                        case 3:
                            // Discount Amount
                            break;
                        case 4:
                            // Cost Plus a Percentage
                            break;
                        case 5:
                            // Cost Plus Fixed Amount
                            break;
                        case 6:
                            // Fixed Price
                            pricePossibility.PRICEPOSSIBILITY3 = Convert.ToDecimal(rdrPricePossibility3["FIXPRICE"].ToString());
                            break;
                    }

                    pricePossibility.PP3_AUDTDATE =  (DateTime)rdrPricePossibility3["AUDTDATE"];

                }

                cmdGetPricePossibility3.Dispose();
                rdrPricePossibility3.Close();



                SqlCommand cmdGetPricePossibility4 = new SqlCommand("SELECT ARCUS.IDCUST, ICCUPR.PRICEBY, ICCUPR.CATEGORY, ICCUPR.ITEMNO, " +
                "ICCUPR.PRICELIST, ICCUPR.PRICETYPE, ICCUPR.DISCPER, ICCUPR.FIXPRICE, " +
                "ICCUPR.STARTDATE, ICCUPR.USELOWEST, ICCUPR.EXPIRE, DATEADD(mi, DATEDIFF(mi, GETUTCDATE(), GETDATE()),CONVERT(VARCHAR(10), CONVERT(Date, CONVERT(VARCHAR(8), ICCUPR.AUDTDATE))) + ' ' +LEFT(LEFT(Right('00000000' + Convert(varchar(8), ICCUPR.AUDTTIME), 8), 4), 2) + ':' +RIGHT(LEFT(right('00000000' + Convert(varchar(8), ICCUPR.AUDTTIME), 8), 4), 2)+ ':' + RIGHT(LEFT(right('00000000' + Convert(varchar(8), ICCUPR.AUDTTIME), 8), 6), 2)) AS AUDTDATE " +
                "FROM ARCUS INNER JOIN ICCUPR ON ARCUS.IDCUST = ICCUPR.CUSTNO " +
                "WHERE(dbo.ARCUS.IDCUST = '" + IDCUST + "') AND ICCUPR.CATEGORY = '" + pricePossibility.CATEGORY + "' AND(dbo.ICCUPR.PRICEBY = 1)", con);

                SqlDataReader rdrPricePossibility4 = cmdGetPricePossibility4.ExecuteReader();

                while (rdrPricePossibility4.Read())
                {

                    if (rdrPricePossibility4["STARTDATE"].ToString() == "0")
                    {
                        pricePossibility.PP4UseStartDate = false;
                        pricePossibility.PP4STARTDATE = Convert.ToDateTime("01/01/2000");
                    }
                    else
                    {
                        pricePossibility.PP4UseStartDate = true;
                        pricePossibility.PP4STARTDATE = DateTime.ParseExact(rdrPricePossibility4["STARTDATE"].ToString(), "yyyyMMdd", null);
                    }

                    if (rdrPricePossibility4["EXPIRE"].ToString() == "0")
                    {
                        pricePossibility.PP4UseExpire = false;
                        pricePossibility.PP4EXPIRE = Convert.ToDateTime("01/01/2000");
                    }
                    else
                    {
                        pricePossibility.PP4UseExpire = true;
                        pricePossibility.PP4EXPIRE = DateTime.ParseExact(rdrPricePossibility4["EXPIRE"].ToString(), "yyyyMMdd", null);
                    }

                    if (Convert.ToDecimal(rdrPricePossibility4["DISCPER"].ToString()) != 0)
                    {
                        if (pricePossibility.PP4UseStartDate)
                        {
                            if (pricePossibility.PP4UseExpire)
                            {
                                if (DateTime.Now <= pricePossibility.PP4EXPIRE)
                                {

                                    pricePossibility.PRICEPOSSIBILITY4 =
                                    pricePossibility.UNITPRICE - (Convert.ToDecimal(rdrPricePossibility4["DISCPER"].ToString()) / 100 * pricePossibility.UNITPRICE);

                                }
                                else
                                {
                                    pricePossibility.PRICEPOSSIBILITY4 = 0;
                                }
                            }
                            else
                            {
                                pricePossibility.PRICEPOSSIBILITY4 =
                                    pricePossibility.UNITPRICE - (Convert.ToDecimal(rdrPricePossibility4["DISCPER"].ToString()) / 100 * pricePossibility.UNITPRICE);

                            }
                        }

                    }
                    else
                    {
                        pricePossibility.PRICEPOSSIBILITY4 = 0;
                    }

                    pricePossibility.PP4_AUDTDATE = (DateTime) rdrPricePossibility4["AUDTDATE"];
                }

                cmdGetPricePossibility4.Dispose();
                rdrPricePossibility4.Close();

                con.Close();


                // check for in sales period 

                ItemPriceByCustomer itemPriceByCustomer = new ItemPriceByCustomer();

                itemPriceByCustomer.IDCUST = IDCUST;
                itemPriceByCustomer.ITEMNO = ITEMNO;

                // get the cheapest price 


                decimal price = 0;


                if (pricePossibility.PRICEPOSSIBILITY1 > 0)
                {
                    price = pricePossibility.PRICEPOSSIBILITY1;
                }

                if (pricePossibility.PRICEPOSSIBILITY2 > 0 && pricePossibility.PRICEPOSSIBILITY2 < price && pricePossibility.SALEEND >= DateTime.Now && pricePossibility.SALESTART <= DateTime.Now)
                {
                    price = pricePossibility.PRICEPOSSIBILITY2;
                }

                if (pricePossibility.PRICEPOSSIBILITY3 > 0 && pricePossibility.PRICEPOSSIBILITY3 < price)
                {
                    price = pricePossibility.PRICEPOSSIBILITY3;
                }

                if (pricePossibility.PRICEPOSSIBILITY4 > 0 && pricePossibility.PRICEPOSSIBILITY4 < price)
                {
                    price = pricePossibility.PRICEPOSSIBILITY4;
                }

                if (pricePossibility.USELOWESTPRICE == false)
                {
                    if (pricePossibility.PRICEPOSSIBILITY3 > 0)
                    {
                        price = pricePossibility.PRICEPOSSIBILITY3;
                    }
                }

                pricePossibility.PRICE = price;

                itemPriceByCustomer.PRICE = pricePossibility.PRICE.ToString();

                if (isActiveAndAllowedOnWeb == false)
                {
                    itemPriceByCustomer.ITEMNO = "INVALID - Inactive AND OR Not Allowed On Web";
                    itemPriceByCustomer.PRICE = null;

                }

                itemPriceByCustomer.BASEPRICE_AUDTDATE = pricePossibility.BASEPRICE_AUDTDATE;
                // DateTime.ParseExact(rdrPricePossibility1["SALESTART"].ToString(),"yyyyMMdd",null);
                itemPriceByCustomer.CUSTOMER_AUDTDATE = customerType.CUSTOMER_AUDTDATE;
                //itemPriceByCustomer.CUSTOMER_AUDTTIME = customerType.CUSTOMER_AUDTTIME; // customerType.CUSTOMER_AUDTDATE;  customerType.CUSTOMER_AUDTTIME;
                itemPriceByCustomer.PP1_AUDTDATE = pricePossibility.PP1_AUDTDATE;
                itemPriceByCustomer.PP2_AUDTDATE = pricePossibility.PP2_AUDTDATE;
                itemPriceByCustomer.PP3_AUDTDATE = pricePossibility.PP3_AUDTDATE;
                itemPriceByCustomer.PP4_AUDTDATE = pricePossibility.PP4_AUDTDATE;
                prices.Add(itemPriceByCustomer);


            }

            return prices.ToList();

        }

        [ApiVersion("3")]
        [HttpGet("{IDCUST}")]
        public List<ItemPriceByCustomer> GetAll(string IDCUST)
        {
            List<ItemPriceByCustomer> prices = new List<ItemPriceByCustomer>();

            string CS = _configuration.GetConnectionString("PricingConnection").ToString();

            //  List<SageCustomer> customers = new List<SageCustomer>();







            String ITEMNO = "";
            using (SqlConnection con = new SqlConnection(CS))
            {

                Boolean isActiveAndAllowedOnWeb = false;

                con.Open();

                String SQLQuery = "SELECT ITEMNO FROM ICITEM WHERE ALLOWONWEB = 1 AND INACTIVE = 0 ";

                DataSet dsItems = new DataSet("ITEMS");

                SqlDataAdapter daItems = new SqlDataAdapter(SQLQuery, con);

                daItems.Fill(dsItems, "ITEMS");

                foreach (DataTable table in dsItems.Tables)
                {
                    foreach (DataRow rdr in table.Rows)
                    {
                        //Run through each item'
                        ITEMNO = rdr["ITEMNO"].ToString();

                        // get IDCUST price level 


                        SqlCommand cmdGetIsValid = new SqlCommand("SELECT ITEMNO, INACTIVE, ALLOWONWEB FROM ICITEM WHERE(ITEMNO = '" + ITEMNO + "') AND(INACTIVE = 0) AND(ALLOWONWEB = 1)", con);

                        cmdGetIsValid.CommandType = CommandType.Text;


                        SqlDataReader rdrIsValid = cmdGetIsValid.ExecuteReader();

                        if ((rdrIsValid.HasRows) == false)
                        {
                            isActiveAndAllowedOnWeb = false;
                        }
                        else
                        {
                            isActiveAndAllowedOnWeb = true;
                        }

                        cmdGetIsValid.Dispose();
                        rdrIsValid.Close();


                        SqlCommand cmdGetPriceLevel = new SqlCommand("SELECT IDCUST, PRICLIST, CUSTTYPE, DATEADD(mi, DATEDIFF(mi, GETUTCDATE(), GETDATE()),CONVERT(VARCHAR(10), CONVERT(Date, CONVERT(VARCHAR(8), AUDTDATE))) + ' ' +LEFT(LEFT(Right('00000000' + Convert(varchar(8), AUDTTIME), 8), 4), 2) + ':' +RIGHT(LEFT(right('00000000' + Convert(varchar(8), AUDTTIME), 8), 4), 2)+ ':' + RIGHT(LEFT(right('00000000' + Convert(varchar(8), AUDTTIME), 8), 6), 2)) AS AUDTDATE FROM ARCUS WHERE IDCUST = '" + IDCUST + "'", con);
                        cmdGetPriceLevel.CommandType = CommandType.Text;
                        // con.Open();

                        SqlDataReader rdrPriceLevel = cmdGetPriceLevel.ExecuteReader();

                        var customerType = new SageCustomerPriceListType();
                        while (rdrPriceLevel.Read())
                        {
                            customerType.IDCUST = IDCUST;
                            customerType.PRICLIST = rdrPriceLevel["PRICLIST"].ToString();
                            customerType.CUSTTYPE = rdrPriceLevel["CUSTTYPE"].ToString();
                            customerType.CUSTOMER_AUDTDATE = (DateTime)rdrPriceLevel["AUDTDATE"]; //, "yyyyMMdd", null); // rdrPriceLevel["AUDTDATE"].ToString();
                                                                                                   //customerType.CUSTOMER_AUDTTIME = rdrPriceLevel["AUDTTIME"].ToString();//, "hh:mm", null); //rdrPriceLevel["AUDTDATE"].ToString(); rdrPriceLevel["AUDTTIME"].ToString();
                        }
                        //con.Close();

                        cmdGetPriceLevel.Dispose();
                        rdrPriceLevel.Close();

                        var pricePossibility = new SagePricePossibility();

                        SqlCommand cmdGetAUDTDATE = new SqlCommand("SELECT DATEADD(mi, DATEDIFF(mi, GETUTCDATE(), GETDATE()),CONVERT(VARCHAR(10), CONVERT(Date, CONVERT(VARCHAR(8), AUDTDATE))) + ' ' +LEFT(LEFT(Right('00000000' + Convert(varchar(8), AUDTTIME), 8), 4), 2) + ':' +RIGHT(LEFT(right('00000000' + Convert(varchar(8), AUDTTIME), 8), 4), 2)+ ':' + RIGHT(LEFT(right('00000000' + Convert(varchar(8), AUDTTIME), 8), 6), 2)) AS AUDTDATE FROM ICPRIC WHERE PRICELIST = 'WEB' AND ITEMNO = '" + ITEMNO + "'", con);
                        SqlDataReader rdrAUDDATE = cmdGetAUDTDATE.ExecuteReader();
                        while (rdrAUDDATE.Read())
                        {
                            pricePossibility.BASEPRICE_AUDTDATE =  (DateTime)rdrAUDDATE["AUDTDATE"]; //, "yyyyMMdd", null);
                        }
                        cmdGetAUDTDATE.Dispose();
                        rdrAUDDATE.Close();

                        // customerType now contains all we need to move to next step

                        SqlCommand cmdGetPricePossibility1 = new SqlCommand("SELECT ICPRICP.ITEMNO, ICPRICP.PRICELIST, ICPRICP.DPRICETYPE," +
                            " ICPRICP.UNITPRICE, ICPRICP.SALESTART, ICPRICP.SALEEND, ICPRIC.PRICETYPE, ICPRIC.PRCNTLVL1,ICPRIC.PRCNTLVL2," +
                            " ICPRIC.PRCNTLVL3, ICPRIC.PRCNTLVL4, ICPRIC.PRCNTLVL5, ARCUS.IDCUST, ARCUS.TEXTSTRE2, ARCUS.PRICLIST," +
                            " ARCUS.CUSTTYPE,ICITEM.CATEGORY , DATEADD(mi, DATEDIFF(mi, GETUTCDATE(), GETDATE()),CONVERT(VARCHAR(10), CONVERT(Date, CONVERT(VARCHAR(8), ICPRICP.AUDTDATE))) + ' ' +LEFT(LEFT(Right('00000000' + Convert(varchar(8), ICPRICP.AUDTTIME), 8), 4), 2) + ':' +RIGHT(LEFT(right('00000000' + Convert(varchar(8), ICPRICP.AUDTTIME), 8), 4), 2)+ ':' + RIGHT(LEFT(right('00000000' + Convert(varchar(8), ICPRICP.AUDTTIME), 8), 6), 2)) AS AUDTDATE FROM ICPRICP INNER JOIN ICPRIC ON ICPRICP.ITEMNO = ICPRIC.ITEMNO AND ICPRICP.PRICELIST = ICPRIC.PRICELIST" +
                            " INNER JOIN ARCUS ON ICPRICP.PRICELIST = ARCUS.PRICLIST INNER JOIN " +
                            " ICITEM ON ICPRICP.ITEMNO = ICITEM.ITEMNO" +
                            " WHERE (ICPRICP.ITEMNO = '" + ITEMNO + "') " +
                            " AND (ARCUS.IDCUST = '" + IDCUST + "') AND (ICPRICP.DPRICETYPE = 1)", con);

                        SqlDataReader rdrPricePossibility1 = cmdGetPricePossibility1.ExecuteReader();



                        while (rdrPricePossibility1.Read())
                        {

                            pricePossibility.IDCUST = IDCUST;
                            pricePossibility.ITEMNO = ITEMNO;
                            pricePossibility.PRICETYPE = 1;
                            pricePossibility.PRICLIST = rdrPricePossibility1["PRICLIST"].ToString();
                            pricePossibility.UNITPRICE = Convert.ToDecimal(rdrPricePossibility1["UNITPRICE"].ToString());
                            if (rdrPricePossibility1["SALESTART"].ToString() == "0")
                            {
                                pricePossibility.SALESTART = Convert.ToDateTime("01/01/2000");
                            }
                            else
                            {
                                pricePossibility.SALESTART = DateTime.ParseExact(rdrPricePossibility1["SALESTART"].ToString(), "yyyyMMdd", null);
                            }
                            if (rdrPricePossibility1["SALEEND"].ToString() == "0")
                            {
                                pricePossibility.SALEEND = Convert.ToDateTime("01/01/2999");
                            }
                            else
                            {
                                pricePossibility.SALEEND = DateTime.ParseExact(rdrPricePossibility1["SALEEND"].ToString(), "yyyyMMdd", null);
                            }

                            pricePossibility.PRCNTLVL1 = Convert.ToDecimal(rdrPricePossibility1["PRCNTLVL1"].ToString());
                            pricePossibility.PRCNTLVL2 = Convert.ToDecimal(rdrPricePossibility1["PRCNTLVL2"].ToString());
                            pricePossibility.PRCNTLVL3 = Convert.ToDecimal(rdrPricePossibility1["PRCNTLVL3"].ToString());
                            pricePossibility.PRCNTLVL4 = Convert.ToDecimal(rdrPricePossibility1["PRCNTLVL4"].ToString());
                            pricePossibility.PRCNTLVL5 = Convert.ToDecimal(rdrPricePossibility1["PRCNTLVL5"].ToString());

                            pricePossibility.CUSTTYPE = Convert.ToInt32(rdrPricePossibility1["CUSTTYPE"].ToString());

                            pricePossibility.CATEGORY = rdrPricePossibility1["CATEGORY"].ToString();

                            pricePossibility.PP1_AUDTDATE =  (DateTime)rdrPricePossibility1["AUDTDATE"];

                        }


                        // Apply PricePossibility1 business rule 1

                        //                Price would be UNITPRICE *(1 - CUSTOMERLEVEL)
                        //IF CUSTTYPE = 0 then 0 else
                        //                    IF CUSTTYPE = 1 then CUSTOMERLEVEL = PRCNTLVL1
                        //IF CUSTTYPE = 2 then CUSTOMERLEVEL = PRCNTLVL2
                        //IF CUSTTYPE = 3 then CUSTOMERLEVEL = PRCNTLVL3
                        //IF CUSTTYPE = 4 then CUSTOMERLEVEL = PRCNTLVL4
                        //IF CUSTTYPE = 5 then CUSTOMERLEVEL = PRCNTLVL5

                        switch (pricePossibility.CUSTTYPE)
                        {
                            case 0:
                                pricePossibility.PRICEPOSSIBILITY1 = pricePossibility.UNITPRICE;
                                break;
                            case 1:
                                pricePossibility.PRICEPOSSIBILITY1 = pricePossibility.UNITPRICE * (1 - pricePossibility.PRCNTLVL1 / 100);
                                break;
                            case 2:
                                pricePossibility.PRICEPOSSIBILITY1 = pricePossibility.UNITPRICE * (1 - pricePossibility.PRCNTLVL2 / 100);
                                break;
                            case 3:
                                pricePossibility.PRICEPOSSIBILITY1 = pricePossibility.UNITPRICE * (1 - pricePossibility.PRCNTLVL3 / 100);
                                break;
                            case 4:
                                pricePossibility.PRICEPOSSIBILITY1 = pricePossibility.UNITPRICE * (1 - pricePossibility.PRCNTLVL4 / 100);
                                break;
                            case 5:
                                pricePossibility.PRICEPOSSIBILITY1 = pricePossibility.UNITPRICE * (1 - pricePossibility.PRCNTLVL5 / 100);
                                break;
                            default:
                                break;
                        }


                        cmdGetPricePossibility1.Dispose();
                        rdrPricePossibility1.Close();

                        SqlCommand cmdGetPricePossibility2 = new SqlCommand("SELECT ICPRICP.ITEMNO, ICPRICP.PRICELIST, ICPRICP.DPRICETYPE, " +
                            " ICPRICP.UNITPRICE, ICPRICP.SALESTART, ICPRICP.SALEEND, ICPRIC.PRICETYPE, ICPRIC.PRCNTLVL1,ICPRIC.PRCNTLVL2," +
                            " ICPRIC.PRCNTLVL3, ICPRIC.PRCNTLVL4, ICPRIC.PRCNTLVL5, ARCUS.IDCUST, ARCUS.TEXTSTRE2, ARCUS.PRICLIST," +
                            " ARCUS.CUSTTYPE, DATEADD(mi, DATEDIFF(mi, GETUTCDATE(), GETDATE()),CONVERT(VARCHAR(10), CONVERT(Date, CONVERT(VARCHAR(8), ICPRICP.AUDTDATE))) + ' ' +LEFT(LEFT(Right('00000000' + Convert(varchar(8), ICPRICP.AUDTTIME), 8), 4), 2) + ':' +RIGHT(LEFT(right('00000000' + Convert(varchar(8), ICPRICP.AUDTTIME), 8), 4), 2)+ ':' + RIGHT(LEFT(right('00000000' + Convert(varchar(8), ICPRICP.AUDTTIME), 8), 6), 2)) AS AUDTDATE FROM ICPRICP INNER JOIN ICPRIC ON ICPRICP.ITEMNO = ICPRIC.ITEMNO AND ICPRICP.PRICELIST = ICPRIC.PRICELIST" +
                            " INNER JOIN ARCUS ON ICPRICP.PRICELIST = ARCUS.PRICLIST WHERE (ICPRICP.ITEMNO = '" + ITEMNO + "') " +
                            " AND (ARCUS.IDCUST = '" + IDCUST + "') AND (ICPRICP.DPRICETYPE = 2)", con);

                        SqlDataReader rdrPricePossibility2 = cmdGetPricePossibility2.ExecuteReader();

                        while (rdrPricePossibility2.Read())
                        {

                            pricePossibility.PRICEPOSSIBILITY2 = Convert.ToDecimal(rdrPricePossibility2["UNITPRICE"].ToString());

                            if (rdrPricePossibility2["SALESTART"].ToString() == "0")
                            {
                                pricePossibility.SALESTART = Convert.ToDateTime("01/01/2000");
                            }
                            else
                            {
                                pricePossibility.SALESTART = DateTime.ParseExact(rdrPricePossibility2["SALESTART"].ToString(), "yyyyMMdd", null);
                            }
                            if (rdrPricePossibility2["SALEEND"].ToString() == "0")
                            {
                                pricePossibility.SALEEND = Convert.ToDateTime("01/01/2000");
                            }
                            else
                            {
                                pricePossibility.SALEEND = DateTime.ParseExact(rdrPricePossibility2["SALEEND"].ToString(), "yyyyMMdd", null);
                            }

                            pricePossibility.PP2_AUDTDATE =  (DateTime)rdrPricePossibility2["AUDTDATE"];

                        }



                        cmdGetPricePossibility2.Dispose();
                        rdrPricePossibility2.Close();


                        SqlCommand cmdGetPricePossibility3 = new SqlCommand("SELECT ARCUS.IDCUST, ICCUPR.PRICEBY, ICCUPR.CATEGORY, ICCUPR.ITEMNO," +
                            " ICCUPR.PRICELIST, ICCUPR.PRICETYPE, ICCUPR.DISCPER, ICCUPR.FIXPRICE,ICCUPR.STARTDATE, ICCUPR.USELOWEST, ICCUPR.EXPIRE, DATEADD(mi, DATEDIFF(mi, GETUTCDATE(), GETDATE()),CONVERT(VARCHAR(10), CONVERT(Date, CONVERT(VARCHAR(8), ICCUPR.AUDTDATE))) + ' ' +LEFT(LEFT(Right('00000000' + Convert(varchar(8), ICCUPR.AUDTTIME), 8), 4), 2) + ':' +RIGHT(LEFT(right('00000000' + Convert(varchar(8), ICCUPR.AUDTTIME), 8), 4), 2)+ ':' + RIGHT(LEFT(right('00000000' + Convert(varchar(8), ICCUPR.AUDTTIME), 8), 6), 2)) AS AUDTDATE FROM ARCUS" +
                            " INNER JOIN ICCUPR ON ARCUS.IDCUST = ICCUPR.CUSTNO WHERE (ARCUS.IDCUST = '" + IDCUST + "') AND ICCUPR.ITEMNO = '" + pricePossibility.ITEMNO + "' AND (ICCUPR.PRICEBY = 2)", con);

                        SqlDataReader rdrPricePossibility3 = cmdGetPricePossibility3.ExecuteReader();

                        while (rdrPricePossibility3.Read())
                        {
                            if (rdrPricePossibility3["USELOWEST"].ToString() == "0")
                            {

                                pricePossibility.USELOWESTPRICE = false;
                            }
                            else
                            {
                                pricePossibility.USELOWESTPRICE = true;
                            }
                            pricePossibility.PRICETYPE = Convert.ToInt32(rdrPricePossibility3["PRICETYPE"].ToString());
                            pricePossibility.DISCPER = Convert.ToDecimal(rdrPricePossibility3["DISCPER"].ToString());
                            pricePossibility.FIXPRICE = Convert.ToDecimal(rdrPricePossibility3["FIXPRICE"].ToString());
                            if (rdrPricePossibility3["STARTDATE"].ToString() == "0")
                            {
                                pricePossibility.STARTDATE = Convert.ToDateTime("01/01/2000");
                            }
                            else
                            {
                                pricePossibility.STARTDATE = DateTime.ParseExact(rdrPricePossibility3["STARTDATE"].ToString(), "yyyyMMdd", null);
                            }

                            if (rdrPricePossibility3["EXPIRE"].ToString() == "0")
                            {
                                pricePossibility.EXPIRE = Convert.ToDateTime("01/01/2999");
                            }
                            else
                            {
                                pricePossibility.EXPIRE = DateTime.ParseExact(rdrPricePossibility3["EXPIRE"].ToString(), "yyyyMMdd", null);
                            }



                            switch (pricePossibility.PRICETYPE)
                            {
                                case 0:
                                    break;
                                case 1:
                                    // Customer Type
                                    break;
                                case 2:
                                    // Discount Percentage
                                    if (pricePossibility.EXPIRE <= DateTime.Today)
                                    {

                                    }
                                    else
                                    {
                                        pricePossibility.PRICEPOSSIBILITY3 = pricePossibility.UNITPRICE - (pricePossibility.DISCPER / 100 * pricePossibility.UNITPRICE);
                                    }
                                    break;
                                case 3:
                                    // Discount Amount
                                    break;
                                case 4:
                                    // Cost Plus a Percentage
                                    break;
                                case 5:
                                    // Cost Plus Fixed Amount
                                    break;
                                case 6:
                                    // Fixed Price
                                    pricePossibility.PRICEPOSSIBILITY3 = Convert.ToDecimal(rdrPricePossibility3["FIXPRICE"].ToString());
                                    break;
                            }

                            pricePossibility.PP3_AUDTDATE =  (DateTime)rdrPricePossibility3["AUDTDATE"];

                        }

                        cmdGetPricePossibility3.Dispose();
                        rdrPricePossibility3.Close();



                        SqlCommand cmdGetPricePossibility4 = new SqlCommand("SELECT ARCUS.IDCUST, ICCUPR.PRICEBY, ICCUPR.CATEGORY, ICCUPR.ITEMNO, " +
                        "ICCUPR.PRICELIST, ICCUPR.PRICETYPE, ICCUPR.DISCPER, ICCUPR.FIXPRICE, " +
                        "ICCUPR.STARTDATE, ICCUPR.USELOWEST, ICCUPR.EXPIRE, DATEADD(mi, DATEDIFF(mi, GETUTCDATE(), GETDATE()),CONVERT(VARCHAR(10), CONVERT(Date, CONVERT(VARCHAR(8), ICCUPR.AUDTDATE))) + ' ' +LEFT(LEFT(Right('00000000' + Convert(varchar(8), ICCUPR.AUDTTIME), 8), 4), 2) + ':' +RIGHT(LEFT(right('00000000' + Convert(varchar(8), ICCUPR.AUDTTIME), 8), 4), 2)+ ':' + RIGHT(LEFT(right('00000000' + Convert(varchar(8), ICCUPR.AUDTTIME), 8), 6), 2)) AS AUDTDATE " +
                        "FROM ARCUS INNER JOIN ICCUPR ON ARCUS.IDCUST = ICCUPR.CUSTNO " +
                        "WHERE(dbo.ARCUS.IDCUST = '" + IDCUST + "') AND ICCUPR.CATEGORY = '" + pricePossibility.CATEGORY + "' AND(dbo.ICCUPR.PRICEBY = 1)", con);

                        SqlDataReader rdrPricePossibility4 = cmdGetPricePossibility4.ExecuteReader();

                        while (rdrPricePossibility4.Read())
                        {

                            if (rdrPricePossibility4["STARTDATE"].ToString() == "0")
                            {
                                pricePossibility.PP4UseStartDate = false;
                                pricePossibility.PP4STARTDATE = Convert.ToDateTime("01/01/2000");
                            }
                            else
                            {
                                pricePossibility.PP4UseStartDate = true;
                                pricePossibility.PP4STARTDATE = DateTime.ParseExact(rdrPricePossibility4["STARTDATE"].ToString(), "yyyyMMdd", null);
                            }

                            if (rdrPricePossibility4["EXPIRE"].ToString() == "0")
                            {
                                pricePossibility.PP4UseExpire = false;
                                pricePossibility.PP4EXPIRE = Convert.ToDateTime("01/01/2000");
                            }
                            else
                            {
                                pricePossibility.PP4UseExpire = true;
                                pricePossibility.PP4EXPIRE = DateTime.ParseExact(rdrPricePossibility4["EXPIRE"].ToString(), "yyyyMMdd", null);
                            }

                            if (Convert.ToDecimal(rdrPricePossibility4["DISCPER"].ToString()) != 0)
                            {
                                if (pricePossibility.PP4UseStartDate)
                                {
                                    if (pricePossibility.PP4UseExpire)
                                    {
                                        if (DateTime.Now <= pricePossibility.PP4EXPIRE)
                                        {

                                            pricePossibility.PRICEPOSSIBILITY4 =
                                            pricePossibility.UNITPRICE - (Convert.ToDecimal(rdrPricePossibility4["DISCPER"].ToString()) / 100 * pricePossibility.UNITPRICE);

                                        }
                                        else
                                        {
                                            pricePossibility.PRICEPOSSIBILITY4 = 0;
                                        }
                                    }
                                    else
                                    {
                                        pricePossibility.PRICEPOSSIBILITY4 =
                                            pricePossibility.UNITPRICE - (Convert.ToDecimal(rdrPricePossibility4["DISCPER"].ToString()) / 100 * pricePossibility.UNITPRICE);

                                    }
                                }

                            }
                            else
                            {
                                pricePossibility.PRICEPOSSIBILITY4 = 0;
                            }

                            pricePossibility.PP4_AUDTDATE =  (DateTime)rdrPricePossibility4["AUDTDATE"];
                        }

                        cmdGetPricePossibility4.Dispose();
                        rdrPricePossibility4.Close();




                        // check for in sales period 

                        ItemPriceByCustomer itemPriceByCustomer = new ItemPriceByCustomer();

                        itemPriceByCustomer.IDCUST = IDCUST;
                        itemPriceByCustomer.ITEMNO = ITEMNO;

                        // get the cheapest price 


                        decimal price = 0;


                        if (pricePossibility.PRICEPOSSIBILITY1 > 0)
                        {
                            price = pricePossibility.PRICEPOSSIBILITY1;
                        }

                        if (pricePossibility.PRICEPOSSIBILITY2 > 0 && pricePossibility.PRICEPOSSIBILITY2 < price && pricePossibility.SALEEND >= DateTime.Now && pricePossibility.SALESTART <= DateTime.Now)
                        {
                            price = pricePossibility.PRICEPOSSIBILITY2;
                        }

                        if (pricePossibility.PRICEPOSSIBILITY3 > 0 && pricePossibility.PRICEPOSSIBILITY3 < price)
                        {
                            price = pricePossibility.PRICEPOSSIBILITY3;
                        }

                        if (pricePossibility.PRICEPOSSIBILITY4 > 0 && pricePossibility.PRICEPOSSIBILITY4 < price)
                        {
                            price = pricePossibility.PRICEPOSSIBILITY4;
                        }

                        if (pricePossibility.USELOWESTPRICE == false)
                        {
                            if (pricePossibility.PRICEPOSSIBILITY3 > 0)
                            {
                                price = pricePossibility.PRICEPOSSIBILITY3;
                            }
                        }

                        pricePossibility.PRICE = price;

                        itemPriceByCustomer.PRICE = pricePossibility.PRICE.ToString();

                        if (isActiveAndAllowedOnWeb == false)
                        {
                            itemPriceByCustomer.ITEMNO = "INVALID - Inactive AND OR Not Allowed On Web";
                            itemPriceByCustomer.PRICE = null;

                        }

                        itemPriceByCustomer.BASEPRICE_AUDTDATE = pricePossibility.BASEPRICE_AUDTDATE;
                        // DateTime.ParseExact(rdrPricePossibility1["SALESTART"].ToString(),"yyyyMMdd",null);
                        itemPriceByCustomer.CUSTOMER_AUDTDATE = customerType.CUSTOMER_AUDTDATE;
                        //itemPriceByCustomer.CUSTOMER_AUDTTIME = customerType.CUSTOMER_AUDTTIME; // customerType.CUSTOMER_AUDTDATE;  customerType.CUSTOMER_AUDTTIME;
                        itemPriceByCustomer.PP1_AUDTDATE = pricePossibility.PP1_AUDTDATE;
                        itemPriceByCustomer.PP2_AUDTDATE = pricePossibility.PP2_AUDTDATE;
                        itemPriceByCustomer.PP3_AUDTDATE = pricePossibility.PP3_AUDTDATE;
                        itemPriceByCustomer.PP4_AUDTDATE = pricePossibility.PP4_AUDTDATE;
                        prices.Add(itemPriceByCustomer);


                    }


                }

                // Check if item is active and is allowed on web


                //SqlCommand cmdGetItems = new SqlCommand("SELECT ITEMNO FROM ICITEM WHERE ALLOWONWEB = 1 AND INACTIVE = 0 ", con);

                //SqlDataReader rdrItems = cmdGetItems.ExecuteReader();

                con.Close();


            }

            return prices.ToList();

        }

        [ApiVersion("3")]
        [MapToApiVersion("3")]
        [HttpGet("Changed/{IDCUST},{DATETIME}")]
        public List<ItemPriceByCustomer> GetAllChanged(string IDCUST, DateTime DATETIME)
        {
            List<ItemPriceByCustomer> prices = new List<ItemPriceByCustomer>();

            string CS = _configuration.GetConnectionString("PricingConnection").ToString();

            //  List<SageCustomer> customers = new List<SageCustomer>();


            DATETIME = DATETIME.AddHours(-10);
            string strHours = DATETIME.Hour.ToString();
            string strMinutes = DATETIME.Minute.ToString();
            string strSeconds = DATETIME.Second.ToString();


            if (strHours.Length == 1)
            {
                strHours = "0" + strHours;
            }

            if (strMinutes.Length == 1)
            {
                strMinutes = "0" + strMinutes;
            }

            if (strSeconds.Length == 1)
            {
                strSeconds = "0" + strSeconds;
            }


            string strSageTime = strHours + strMinutes + strSeconds + "00";

            decimal SageTime = Convert.ToDecimal(strSageTime);

            decimal AuditDate;
            decimal AudtitTime;


            DateTime dateTime = DATETIME;

            //string Hours = dateTime.Hour.ToString("00");

            // decimal h = Convert.ToDecimal(dateTime.Hour.ToString("00")) + 10;

            // decimal m = Convert.ToDecimal(dateTime.Minute.ToString("00"));

            //decimal s = Convert.ToDecimal(dateTime.Second.ToString("00"));

            //  AuditDate = Convert.ToDecimal(Convert.ToDecimal(dateTime.Year).ToString() + Convert.ToDecimal(dateTime.Month).ToString("00") + Convert.ToDecimal(dateTime.Day).ToString("00"));



            // AudtitTime = Convert.ToDecimal(h * 100000);// + Convert.ToDecimal(dateTime.Minute.ToString()) * 1000 + Convert.ToDecimal(dateTime.Second).ToString("00") );



            // convert dateTime parameter to Sage GMT

            // string Seconds = dateTime.Second.ToString("00");

            // string Minutes = dateTime.Minute.ToString("00");

            //Hours = dateTime.Hour.ToString("00");

            // decimal SageTime = Convert.ToDecimal(Convert.ToDecimal(Hours).ToString("00") + Convert.ToDecimal(Minutes).ToString("00") + Convert.ToDecimal(Seconds).ToString("00") + Convert.ToDecimal("50").ToString());

            // GMT = AEST - 10 hours 

            // if (Convert.ToDecimal(Hours) < 10 )
            //   {
            // Take a day off and take 10 hours off

            //       decimal GMThours = 24 - Convert.ToDecimal(Hours);



            // dateTime = dateTime.AddHours(-10);

            //dateTime =dateTime.AddHours GMThours;

            AuditDate = Convert.ToDecimal(Convert.ToDecimal(dateTime.Year).ToString() + Convert.ToDecimal(dateTime.Month).ToString("00") + Convert.ToDecimal(dateTime.Day).ToString("00"));
            //  }
            //else
            //  {
            //      dateTime = dateTime.AddHours(- 10);
            //      Hours = dateTime.Hour.ToString();
            //  }

            //            decimal SageTime = Convert.ToDecimal(Convert.ToDecimal(Hours).ToString("00") + Convert.ToDecimal(Minutes).ToString("00") + Convert.ToDecimal(Seconds).ToString("00") + Convert.ToDecimal("50").ToString());

            //string strSageTime =  +  "99";


            String ITEMNO = "";
            using (SqlConnection con = new SqlConnection(CS))
            {

                Boolean isActiveAndAllowedOnWeb = false;

                con.Open();


                /* 
                    SELECT dbo.ICITEM.ITEMNO
                    FROM   dbo.ICITEM LEFT OUTER JOIN
                              dbo.ICPRICP ON dbo.ICITEM.ITEMNO = dbo.ICPRICP.ITEMNO
                    WHERE  (dbo.ICPRICP.PRICELIST = 'RET') 
                    AND (dbo.ICPRICP.AUDTDATE > '20210427') AND (dbo.ICITEM.INACTIVE = 0) AND (dbo.ICITEM.ALLOWONWEB = 1) OR
                              (dbo.ICPRICP.PRICELIST = 'RET') AND (dbo.ICPRICP.AUDTDATE = '20210427') AND (dbo.ICPRICP.AUDTTIME > '1591926') AND (dbo.ICITEM.INACTIVE = 0) AND (dbo.ICITEM.ALLOWONWEB = 1)
                */


                String SQLQuery = "SELECT DISTINCT ICITEM.ITEMNO FROM ICITEM LEFT OUTER JOIN ICPRICP ON ICITEM.ITEMNO = ICPRICP.ITEMNO " +
                    "WHERE (ICPRICP.PRICELIST = 'RET') " +
                    "AND (ICPRICP.AUDTDATE > '" + AuditDate + "') AND INACTIVE = 0 AND ALLOWONWEB = 1" +
                    " OR (ICPRICP.PRICELIST = 'RET') AND (ICPRICP.AUDTDATE = '" + AuditDate + "') AND (ICPRICP.AUDTTIME > '" + SageTime + "') AND INACTIVE = 0 AND ALLOWONWEB = 1";

                DataSet dsItems = new DataSet("ITEMS");

                SqlDataAdapter daItems = new SqlDataAdapter(SQLQuery, con);

                daItems.Fill(dsItems, "ITEMS");

                foreach (DataTable table in dsItems.Tables)
                {
                    foreach (DataRow rdr in table.Rows)
                    {
                        //Run through each item'
                        ITEMNO = rdr["ITEMNO"].ToString();

                        // get IDCUST price level 


                        SqlCommand cmdGetIsValid = new SqlCommand("SELECT ITEMNO, INACTIVE, ALLOWONWEB FROM ICITEM WHERE(ITEMNO = '" + ITEMNO + "') AND(INACTIVE = 0) AND(ALLOWONWEB = 1)", con);

                        cmdGetIsValid.CommandType = CommandType.Text;


                        SqlDataReader rdrIsValid = cmdGetIsValid.ExecuteReader();

                        if ((rdrIsValid.HasRows) == false)
                        {
                            isActiveAndAllowedOnWeb = false;
                        }
                        else
                        {
                            isActiveAndAllowedOnWeb = true;
                        }

                        cmdGetIsValid.Dispose();
                        rdrIsValid.Close();


                        SqlCommand cmdGetPriceLevel = new SqlCommand("SELECT IDCUST, PRICLIST, CUSTTYPE, DATEADD(mi, DATEDIFF(mi, GETUTCDATE(), GETDATE()),CONVERT(VARCHAR(10), CONVERT(Date, CONVERT(VARCHAR(8), AUDTDATE))) + ' ' +LEFT(LEFT(Right('00000000' + Convert(varchar(8), AUDTTIME), 8), 4), 2) + ':' +RIGHT(LEFT(right('00000000' + Convert(varchar(8), AUDTTIME), 8), 4), 2)+ ':' + RIGHT(LEFT(right('00000000' + Convert(varchar(8), AUDTTIME), 8), 6), 2)) AS AUDTDATE FROM ARCUS WHERE IDCUST = '" + IDCUST + "'", con);
                        cmdGetPriceLevel.CommandType = CommandType.Text;
                        // con.Open();

                        SqlDataReader rdrPriceLevel = cmdGetPriceLevel.ExecuteReader();

                        var customerType = new SageCustomerPriceListType();
                        while (rdrPriceLevel.Read())
                        {
                            customerType.IDCUST = IDCUST;
                            customerType.PRICLIST = rdrPriceLevel["PRICLIST"].ToString();
                            customerType.CUSTTYPE = rdrPriceLevel["CUSTTYPE"].ToString();
                            customerType.CUSTOMER_AUDTDATE =  (DateTime)rdrPriceLevel["AUDTDATE"]; //, "yyyyMMdd", null); // rdrPriceLevel["AUDTDATE"].ToString();
                                                                                                   //customerType.CUSTOMER_AUDTTIME = rdrPriceLevel["AUDTTIME"].ToString();//, "hh:mm", null); //rdrPriceLevel["AUDTDATE"].ToString(); rdrPriceLevel["AUDTTIME"].ToString();
                        }
                        //con.Close();

                        cmdGetPriceLevel.Dispose();
                        rdrPriceLevel.Close();

                        var pricePossibility = new SagePricePossibility();

                        SqlCommand cmdGetAUDTDATE = new SqlCommand("SELECT DATEADD(mi, DATEDIFF(mi, GETUTCDATE(), GETDATE()),CONVERT(VARCHAR(10), CONVERT(Date, CONVERT(VARCHAR(8), AUDTDATE))) + ' ' +LEFT(LEFT(Right('00000000' + Convert(varchar(8), AUDTTIME), 8), 4), 2) + ':' +RIGHT(LEFT(right('00000000' + Convert(varchar(8), AUDTTIME), 8), 4), 2)+ ':' + RIGHT(LEFT(right('00000000' + Convert(varchar(8), AUDTTIME), 8), 6), 2)) AS AUDTDATE FROM ICPRIC WHERE PRICELIST = 'WEB' AND ITEMNO = '" + ITEMNO + "'", con);
                        SqlDataReader rdrAUDDATE = cmdGetAUDTDATE.ExecuteReader();
                        while (rdrAUDDATE.Read())
                        {
                            pricePossibility.BASEPRICE_AUDTDATE =  (DateTime)rdrAUDDATE["AUDTDATE"]; //, "yyyyMMdd", null);
                        }
                        cmdGetAUDTDATE.Dispose();
                        rdrAUDDATE.Close();

                        // customerType now contains all we need to move to next step

                        SqlCommand cmdGetPricePossibility1 = new SqlCommand("SELECT ICPRICP.ITEMNO, ICPRICP.PRICELIST, ICPRICP.DPRICETYPE," +
                            " ICPRICP.UNITPRICE, ICPRICP.SALESTART, ICPRICP.SALEEND, ICPRIC.PRICETYPE, ICPRIC.PRCNTLVL1,ICPRIC.PRCNTLVL2," +
                            " ICPRIC.PRCNTLVL3, ICPRIC.PRCNTLVL4, ICPRIC.PRCNTLVL5, ARCUS.IDCUST, ARCUS.TEXTSTRE2, ARCUS.PRICLIST," +
                            " ARCUS.CUSTTYPE,ICITEM.CATEGORY , DATEADD(mi, DATEDIFF(mi, GETUTCDATE(), GETDATE()),CONVERT(VARCHAR(10), CONVERT(Date, CONVERT(VARCHAR(8), ICPRICP.AUDTDATE))) + ' ' +LEFT(LEFT(Right('00000000' + Convert(varchar(8), ICPRICP.AUDTTIME), 8), 4), 2) + ':' +RIGHT(LEFT(right('00000000' + Convert(varchar(8), ICPRICP.AUDTTIME), 8), 4), 2)+ ':' + RIGHT(LEFT(right('00000000' + Convert(varchar(8), ICPRICP.AUDTTIME), 8), 6), 2)) AS AUDTDATE FROM ICPRICP INNER JOIN ICPRIC ON ICPRICP.ITEMNO = ICPRIC.ITEMNO AND ICPRICP.PRICELIST = ICPRIC.PRICELIST" +
                            " INNER JOIN ARCUS ON ICPRICP.PRICELIST = ARCUS.PRICLIST INNER JOIN " +
                            " ICITEM ON ICPRICP.ITEMNO = ICITEM.ITEMNO" +
                            " WHERE (ICPRICP.ITEMNO = '" + ITEMNO + "') " +
                            " AND (ARCUS.IDCUST = '" + IDCUST + "') AND (ICPRICP.DPRICETYPE = 1)", con);

                        SqlDataReader rdrPricePossibility1 = cmdGetPricePossibility1.ExecuteReader();



                        while (rdrPricePossibility1.Read())
                        {

                            pricePossibility.IDCUST = IDCUST;
                            pricePossibility.ITEMNO = ITEMNO;
                            pricePossibility.PRICETYPE = 1;
                            pricePossibility.PRICLIST = rdrPricePossibility1["PRICLIST"].ToString();
                            pricePossibility.UNITPRICE = Convert.ToDecimal(rdrPricePossibility1["UNITPRICE"].ToString());
                            if (rdrPricePossibility1["SALESTART"].ToString() == "0")
                            {
                                pricePossibility.SALESTART = Convert.ToDateTime("01/01/2000");
                            }
                            else
                            {
                                pricePossibility.SALESTART = DateTime.ParseExact(rdrPricePossibility1["SALESTART"].ToString(), "yyyyMMdd", null);
                            }
                            if (rdrPricePossibility1["SALEEND"].ToString() == "0")
                            {
                                pricePossibility.SALEEND = Convert.ToDateTime("01/01/2999");
                            }
                            else
                            {
                                pricePossibility.SALEEND = DateTime.ParseExact(rdrPricePossibility1["SALEEND"].ToString(), "yyyyMMdd", null);
                            }

                            pricePossibility.PRCNTLVL1 = Convert.ToDecimal(rdrPricePossibility1["PRCNTLVL1"].ToString());
                            pricePossibility.PRCNTLVL2 = Convert.ToDecimal(rdrPricePossibility1["PRCNTLVL2"].ToString());
                            pricePossibility.PRCNTLVL3 = Convert.ToDecimal(rdrPricePossibility1["PRCNTLVL3"].ToString());
                            pricePossibility.PRCNTLVL4 = Convert.ToDecimal(rdrPricePossibility1["PRCNTLVL4"].ToString());
                            pricePossibility.PRCNTLVL5 = Convert.ToDecimal(rdrPricePossibility1["PRCNTLVL5"].ToString());

                            pricePossibility.CUSTTYPE = Convert.ToInt32(rdrPricePossibility1["CUSTTYPE"].ToString());

                            pricePossibility.CATEGORY = rdrPricePossibility1["CATEGORY"].ToString();

                            pricePossibility.PP1_AUDTDATE =  (DateTime)rdrPricePossibility1["AUDTDATE"];

                        }


                        // Apply PricePossibility1 business rule 1

                        //                Price would be UNITPRICE *(1 - CUSTOMERLEVEL)
                        //IF CUSTTYPE = 0 then 0 else
                        //                    IF CUSTTYPE = 1 then CUSTOMERLEVEL = PRCNTLVL1
                        //IF CUSTTYPE = 2 then CUSTOMERLEVEL = PRCNTLVL2
                        //IF CUSTTYPE = 3 then CUSTOMERLEVEL = PRCNTLVL3
                        //IF CUSTTYPE = 4 then CUSTOMERLEVEL = PRCNTLVL4
                        //IF CUSTTYPE = 5 then CUSTOMERLEVEL = PRCNTLVL5

                        switch (pricePossibility.CUSTTYPE)
                        {
                            case 0:
                                pricePossibility.PRICEPOSSIBILITY1 = pricePossibility.UNITPRICE;
                                break;
                            case 1:
                                pricePossibility.PRICEPOSSIBILITY1 = pricePossibility.UNITPRICE * (1 - pricePossibility.PRCNTLVL1 / 100);
                                break;
                            case 2:
                                pricePossibility.PRICEPOSSIBILITY1 = pricePossibility.UNITPRICE * (1 - pricePossibility.PRCNTLVL2 / 100);
                                break;
                            case 3:
                                pricePossibility.PRICEPOSSIBILITY1 = pricePossibility.UNITPRICE * (1 - pricePossibility.PRCNTLVL3 / 100);
                                break;
                            case 4:
                                pricePossibility.PRICEPOSSIBILITY1 = pricePossibility.UNITPRICE * (1 - pricePossibility.PRCNTLVL4 / 100);
                                break;
                            case 5:
                                pricePossibility.PRICEPOSSIBILITY1 = pricePossibility.UNITPRICE * (1 - pricePossibility.PRCNTLVL5 / 100);
                                break;
                            default:
                                break;
                        }


                        cmdGetPricePossibility1.Dispose();
                        rdrPricePossibility1.Close();

                        SqlCommand cmdGetPricePossibility2 = new SqlCommand("SELECT ICPRICP.ITEMNO, ICPRICP.PRICELIST, ICPRICP.DPRICETYPE, " +
                            " ICPRICP.UNITPRICE, ICPRICP.SALESTART, ICPRICP.SALEEND, ICPRIC.PRICETYPE, ICPRIC.PRCNTLVL1,ICPRIC.PRCNTLVL2," +
                            " ICPRIC.PRCNTLVL3, ICPRIC.PRCNTLVL4, ICPRIC.PRCNTLVL5, ARCUS.IDCUST, ARCUS.TEXTSTRE2, ARCUS.PRICLIST," +
                            " ARCUS.CUSTTYPE, DATEADD(mi, DATEDIFF(mi, GETUTCDATE(), GETDATE()),CONVERT(VARCHAR(10), CONVERT(Date, CONVERT(VARCHAR(8), ICPRICP.AUDTDATE))) + ' ' +LEFT(LEFT(Right('00000000' + Convert(varchar(8), ICPRICP.AUDTTIME), 8), 4), 2) + ':' +RIGHT(LEFT(right('00000000' + Convert(varchar(8), ICPRICP.AUDTTIME), 8), 4), 2)+ ':' + RIGHT(LEFT(right('00000000' + Convert(varchar(8), ICPRICP.AUDTTIME), 8), 6), 2)) AS AUDTDATE FROM ICPRICP INNER JOIN ICPRIC ON ICPRICP.ITEMNO = ICPRIC.ITEMNO AND ICPRICP.PRICELIST = ICPRIC.PRICELIST" +
                            " INNER JOIN ARCUS ON ICPRICP.PRICELIST = ARCUS.PRICLIST WHERE (ICPRICP.ITEMNO = '" + ITEMNO + "') " +
                            " AND (ARCUS.IDCUST = '" + IDCUST + "') AND (ICPRICP.DPRICETYPE = 2)", con);

                        SqlDataReader rdrPricePossibility2 = cmdGetPricePossibility2.ExecuteReader();

                        while (rdrPricePossibility2.Read())
                        {

                            pricePossibility.PRICEPOSSIBILITY2 = Convert.ToDecimal(rdrPricePossibility2["UNITPRICE"].ToString());

                            if (rdrPricePossibility2["SALESTART"].ToString() == "0")
                            {
                                pricePossibility.SALESTART = Convert.ToDateTime("01/01/2000");
                            }
                            else
                            {
                                pricePossibility.SALESTART = DateTime.ParseExact(rdrPricePossibility2["SALESTART"].ToString(), "yyyyMMdd", null);
                            }
                            if (rdrPricePossibility2["SALEEND"].ToString() == "0")
                            {
                                pricePossibility.SALEEND = Convert.ToDateTime("01/01/2000");
                            }
                            else
                            {
                                pricePossibility.SALEEND = DateTime.ParseExact(rdrPricePossibility2["SALEEND"].ToString(), "yyyyMMdd", null);
                            }

                            pricePossibility.PP2_AUDTDATE =  (DateTime)rdrPricePossibility2["AUDTDATE"];

                        }



                        cmdGetPricePossibility2.Dispose();
                        rdrPricePossibility2.Close();


                        SqlCommand cmdGetPricePossibility3 = new SqlCommand("SELECT ARCUS.IDCUST, ICCUPR.PRICEBY, ICCUPR.CATEGORY, ICCUPR.ITEMNO," +
                            " ICCUPR.PRICELIST, ICCUPR.PRICETYPE, ICCUPR.DISCPER, ICCUPR.FIXPRICE,ICCUPR.STARTDATE, ICCUPR.USELOWEST, ICCUPR.EXPIRE, DATEADD(mi, DATEDIFF(mi, GETUTCDATE(), GETDATE()),CONVERT(VARCHAR(10), CONVERT(Date, CONVERT(VARCHAR(8), ICCUPR.AUDTDATE))) + ' ' +LEFT(LEFT(Right('00000000' + Convert(varchar(8), ICCUPR.AUDTTIME), 8), 4), 2) + ':' +RIGHT(LEFT(right('00000000' + Convert(varchar(8), ICCUPR.AUDTTIME), 8), 4), 2)+ ':' + RIGHT(LEFT(right('00000000' + Convert(varchar(8), ICCUPR.AUDTTIME), 8), 6), 2)) AS AUDTDATE FROM ARCUS" +
                            " INNER JOIN ICCUPR ON ARCUS.IDCUST = ICCUPR.CUSTNO WHERE (ARCUS.IDCUST = '" + IDCUST + "') AND ICCUPR.ITEMNO = '" + pricePossibility.ITEMNO + "' AND (ICCUPR.PRICEBY = 2)", con);

                        SqlDataReader rdrPricePossibility3 = cmdGetPricePossibility3.ExecuteReader();

                        while (rdrPricePossibility3.Read())
                        {
                            if (rdrPricePossibility3["USELOWEST"].ToString() == "0")
                            {

                                pricePossibility.USELOWESTPRICE = false;
                            }
                            else
                            {
                                pricePossibility.USELOWESTPRICE = true;
                            }
                            pricePossibility.PRICETYPE = Convert.ToInt32(rdrPricePossibility3["PRICETYPE"].ToString());
                            pricePossibility.DISCPER = Convert.ToDecimal(rdrPricePossibility3["DISCPER"].ToString());
                            pricePossibility.FIXPRICE = Convert.ToDecimal(rdrPricePossibility3["FIXPRICE"].ToString());
                            if (rdrPricePossibility3["STARTDATE"].ToString() == "0")
                            {
                                pricePossibility.STARTDATE = Convert.ToDateTime("01/01/2000");
                            }
                            else
                            {
                                pricePossibility.STARTDATE = DateTime.ParseExact(rdrPricePossibility3["STARTDATE"].ToString(), "yyyyMMdd", null);
                            }

                            if (rdrPricePossibility3["EXPIRE"].ToString() == "0")
                            {
                                pricePossibility.EXPIRE = Convert.ToDateTime("01/01/2999");
                            }
                            else
                            {
                                pricePossibility.EXPIRE = DateTime.ParseExact(rdrPricePossibility3["EXPIRE"].ToString(), "yyyyMMdd", null);
                            }



                            switch (pricePossibility.PRICETYPE)
                            {
                                case 0:
                                    break;
                                case 1:
                                    // Customer Type
                                    break;
                                case 2:
                                    // Discount Percentage
                                    if (pricePossibility.EXPIRE <= DateTime.Today)
                                    {

                                    }
                                    else
                                    {
                                        pricePossibility.PRICEPOSSIBILITY3 = pricePossibility.UNITPRICE - (pricePossibility.DISCPER / 100 * pricePossibility.UNITPRICE);
                                    }
                                    break;
                                case 3:
                                    // Discount Amount
                                    break;
                                case 4:
                                    // Cost Plus a Percentage
                                    break;
                                case 5:
                                    // Cost Plus Fixed Amount
                                    break;
                                case 6:
                                    // Fixed Price
                                    pricePossibility.PRICEPOSSIBILITY3 = Convert.ToDecimal(rdrPricePossibility3["FIXPRICE"].ToString());
                                    break;
                            }

                            pricePossibility.PP3_AUDTDATE = (DateTime)rdrPricePossibility3["AUDTDATE"];

                        }

                        cmdGetPricePossibility3.Dispose();
                        rdrPricePossibility3.Close();



                        SqlCommand cmdGetPricePossibility4 = new SqlCommand("SELECT ARCUS.IDCUST, ICCUPR.PRICEBY, ICCUPR.CATEGORY, ICCUPR.ITEMNO, " +
                        "ICCUPR.PRICELIST, ICCUPR.PRICETYPE, ICCUPR.DISCPER, ICCUPR.FIXPRICE, " +
                        "ICCUPR.STARTDATE, ICCUPR.USELOWEST, ICCUPR.EXPIRE, DATEADD(mi, DATEDIFF(mi, GETUTCDATE(), GETDATE()),CONVERT(VARCHAR(10), CONVERT(Date, CONVERT(VARCHAR(8), ICCUPR.AUDTDATE))) + ' ' +LEFT(LEFT(Right('00000000' + Convert(varchar(8), ICCUPR.AUDTTIME), 8), 4), 2) + ':' +RIGHT(LEFT(right('00000000' + Convert(varchar(8), ICCUPR.AUDTTIME), 8), 4), 2)+ ':' + RIGHT(LEFT(right('00000000' + Convert(varchar(8), ICCUPR.AUDTTIME), 8), 6), 2)) AS AUDTDATE " +
                        "FROM ARCUS INNER JOIN ICCUPR ON ARCUS.IDCUST = ICCUPR.CUSTNO " +
                        "WHERE(dbo.ARCUS.IDCUST = '" + IDCUST + "') AND ICCUPR.CATEGORY = '" + pricePossibility.CATEGORY + "' AND(dbo.ICCUPR.PRICEBY = 1)", con);

                        SqlDataReader rdrPricePossibility4 = cmdGetPricePossibility4.ExecuteReader();

                        while (rdrPricePossibility4.Read())
                        {

                            if (rdrPricePossibility4["STARTDATE"].ToString() == "0")
                            {
                                pricePossibility.PP4UseStartDate = false;
                                pricePossibility.PP4STARTDATE = Convert.ToDateTime("01/01/2000");
                            }
                            else
                            {
                                pricePossibility.PP4UseStartDate = true;
                                pricePossibility.PP4STARTDATE = DateTime.ParseExact(rdrPricePossibility4["STARTDATE"].ToString(), "yyyyMMdd", null);
                            }

                            if (rdrPricePossibility4["EXPIRE"].ToString() == "0")
                            {
                                pricePossibility.PP4UseExpire = false;
                                pricePossibility.PP4EXPIRE = Convert.ToDateTime("01/01/2000");
                            }
                            else
                            {
                                pricePossibility.PP4UseExpire = true;
                                pricePossibility.PP4EXPIRE = DateTime.ParseExact(rdrPricePossibility4["EXPIRE"].ToString(), "yyyyMMdd", null);
                            }

                            if (Convert.ToDecimal(rdrPricePossibility4["DISCPER"].ToString()) != 0)
                            {
                                if (pricePossibility.PP4UseStartDate)
                                {
                                    if (pricePossibility.PP4UseExpire)
                                    {
                                        if (DateTime.Now <= pricePossibility.PP4EXPIRE)
                                        {

                                            pricePossibility.PRICEPOSSIBILITY4 =
                                            pricePossibility.UNITPRICE - (Convert.ToDecimal(rdrPricePossibility4["DISCPER"].ToString()) / 100 * pricePossibility.UNITPRICE);

                                        }
                                        else
                                        {
                                            pricePossibility.PRICEPOSSIBILITY4 = 0;
                                        }
                                    }
                                    else
                                    {
                                        pricePossibility.PRICEPOSSIBILITY4 =
                                            pricePossibility.UNITPRICE - (Convert.ToDecimal(rdrPricePossibility4["DISCPER"].ToString()) / 100 * pricePossibility.UNITPRICE);

                                    }
                                }

                            }
                            else
                            {
                                pricePossibility.PRICEPOSSIBILITY4 = 0;
                            }

                            pricePossibility.PP4_AUDTDATE = (DateTime)rdrPricePossibility4["AUDTDATE"];
                        }

                        cmdGetPricePossibility4.Dispose();
                        rdrPricePossibility4.Close();




                        // check for in sales period 

                        ItemPriceByCustomer itemPriceByCustomer = new ItemPriceByCustomer();

                        itemPriceByCustomer.IDCUST = IDCUST;
                        itemPriceByCustomer.ITEMNO = ITEMNO;

                        // get the cheapest price 


                        decimal price = 0;


                        if (pricePossibility.PRICEPOSSIBILITY1 > 0)
                        {
                            price = pricePossibility.PRICEPOSSIBILITY1;
                        }

                        if (pricePossibility.PRICEPOSSIBILITY2 > 0 && pricePossibility.PRICEPOSSIBILITY2 < price && pricePossibility.SALEEND >= DateTime.Now && pricePossibility.SALESTART <= DateTime.Now)
                        {
                            price = pricePossibility.PRICEPOSSIBILITY2;
                        }

                        if (pricePossibility.PRICEPOSSIBILITY3 > 0 && pricePossibility.PRICEPOSSIBILITY3 < price)
                        {
                            price = pricePossibility.PRICEPOSSIBILITY3;
                        }

                        if (pricePossibility.PRICEPOSSIBILITY4 > 0 && pricePossibility.PRICEPOSSIBILITY4 < price)
                        {
                            price = pricePossibility.PRICEPOSSIBILITY4;
                        }

                        if (pricePossibility.USELOWESTPRICE == false)
                        {
                            if (pricePossibility.PRICEPOSSIBILITY3 > 0)
                            {
                                price = pricePossibility.PRICEPOSSIBILITY3;
                            }
                        }

                        pricePossibility.PRICE = price;

                        itemPriceByCustomer.PRICE = pricePossibility.PRICE.ToString();

                        if (isActiveAndAllowedOnWeb == false)
                        {
                            itemPriceByCustomer.ITEMNO = "INVALID - Inactive AND OR Not Allowed On Web";
                            itemPriceByCustomer.PRICE = null;

                        }

                        itemPriceByCustomer.BASEPRICE_AUDTDATE = pricePossibility.BASEPRICE_AUDTDATE;
                        // DateTime.ParseExact(rdrPricePossibility1["SALESTART"].ToString(),"yyyyMMdd",null);
                        itemPriceByCustomer.CUSTOMER_AUDTDATE = customerType.CUSTOMER_AUDTDATE;
                        //itemPriceByCustomer.CUSTOMER_AUDTTIME = customerType.CUSTOMER_AUDTTIME; // customerType.CUSTOMER_AUDTDATE;  customerType.CUSTOMER_AUDTTIME;
                        itemPriceByCustomer.PP1_AUDTDATE = pricePossibility.PP1_AUDTDATE;
                        itemPriceByCustomer.PP2_AUDTDATE = pricePossibility.PP2_AUDTDATE;
                        itemPriceByCustomer.PP3_AUDTDATE = pricePossibility.PP3_AUDTDATE;
                        itemPriceByCustomer.PP4_AUDTDATE = pricePossibility.PP4_AUDTDATE;
                        prices.Add(itemPriceByCustomer);


                    }


                }

                // Check if item is active and is allowed on web


                //SqlCommand cmdGetItems = new SqlCommand("SELECT ITEMNO FROM ICITEM WHERE ALLOWONWEB = 1 AND INACTIVE = 0 ", con);

                //SqlDataReader rdrItems = cmdGetItems.ExecuteReader();

                con.Close();


            }

            return prices.ToList();

        }


        




    }
}
