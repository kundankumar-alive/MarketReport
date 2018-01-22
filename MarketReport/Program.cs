using MySql.Data.MySqlClient;
using Newtonsoft.Json.Linq;
using QuandlCS.Connection;
using QuandlCS.Interfaces;
using QuandlCS.Requests;
using QuandlCS.Types;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MarketReport
{
    class Program
    {
        private static string URL = "https://www.quandl.com/api/v3/datasets.json";
        private static string urlParameters = "?database_code=NSE&per_page=100&sort_by=id&page={0}&api_key=XA7e5ChTTpoR92CksrGY";
        static int _truncateDays = 135;
        static void Main(string[] args)
        {
            InsertCompanyDetails();
            //ProcessShareValues();
            //GeTWeeklyReport();

        }
        static void ProcessShareValues()
        {
            List<DataTable> lstTable = new List<DataTable>();
            int limit = 200;
            int offset = 0;
            bool bHasData = true;
            var threads = new List<Thread>();
            while (bHasData)
            {
                DataTable dtCode = GetDataFromTable("mr", "company", "id,company_id,dataset_code", new Dictionary<string, string>(), string.Empty, 200, offset);
                if (dtCode.Rows.Count > 0)
                {
                    int to = offset + dtCode.Rows.Count;
                    var thread = new Thread(() => InsertShare(dtCode))
                    {
                        Name = string.Format("ThreadInsertShare{0}", offset + "-" + to)
                    };
                    thread.Start();
                    threads.Add(thread);
                    offset += limit;
                }
                else
                    bHasData = false;
            }
            threads.ForEach(t => t.Join());
            Console.WriteLine("Done");
            Console.ReadKey();
        }
        static void InsertCompanyDetails()
        {
            HttpClient client = new HttpClient();
            client.BaseAddress = new Uri(URL);
            client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
            bool bIterate = true;
            for (int i = 0; i < 100; i++)
            {
                if (bIterate)
                {
                    HttpResponseMessage response = client.GetAsync(string.Format(urlParameters, i + 1)).Result;
                    if (response.IsSuccessStatusCode)
                    {
                        string res = response.Content.ReadAsStringAsync().Result.ToString();
                        dynamic stuff1 = Newtonsoft.Json.JsonConvert.DeserializeObject(res);
                        bIterate = stuff1["datasets"].Count == 100 ? true : false;
                        List<Dictionary<string, object>> lstDict = new List<Dictionary<string, object>>();
                        Dictionary<string, string> dictStr = new Dictionary<string, string>();
                        int k = 0;
                        for (int j = 0; j < stuff1["datasets"].Count; j++)
                        {
                            var items = stuff1["datasets"][j];
                            Dictionary<string, object> dict = new Dictionary<string, object>();
                            foreach (var property in items.Properties())
                            {
                                dict.Add(property.Name, property.Value);
                                if (k == 0)
                                    dictStr.Add(property.Name, property.Value.Type.ToString());
                            }
                            k += 1;
                            lstDict.Add(dict);
                        }
                        InsertIntoTable("mr", "company", lstDict, dictStr, "id", "id");
                    }
                    else
                    {

                    }
                }
            }
        }
        static void InsertShare(DataTable dtCode)
        {
            Dictionary<string, string> tableStructure = new Dictionary<string, string>(){
                        { "Company_id", "integer" },
                        {"QuandlCompanyId", "integer"},
                        {"DATE","date"},
                        {"OPEN","double"},
                        {"High","double"},
                        {"Low","double"},
                        {"LAST","double"},
                        {"CLOSE","double"},
                        {"Total_Trade_Quantity","double"},
                        {"Turnover","double"},
                        {"Refreshed_At","datetime"}
                };
            //DataTable dtCode = GetDataFromTable("mr", "company", "id,company_id,dataset_code", new Dictionary<string, string>(), string.Empty);
            foreach (DataRow dr in dtCode.Rows)
            {
                QuandlDownloadRequest request = new QuandlDownloadRequest();
                request.APIKey = "XA7e5ChTTpoR92CksrGY";
                DateTime currentTime = DateTime.Now;
                Console.WriteLine(dr["dataset_code"].ToString() + "--" + Thread.CurrentThread.Name + "--" + currentTime);
                request.Datacode = new Datacode("NSE", dr["dataset_code"].ToString());
                request.Format = FileFormats.JSON;
                request.Frequency = Frequencies.Daily;
                request.Sort = SortOrders.Ascending;
                request.Transformation = Transformations.None;
                request.Truncation = _truncateDays;
                request.Sort = SortOrders.Descending;
                string result = GetShareDetailFromQuanDL(request);
                if (string.IsNullOrWhiteSpace(result))
                    continue;
                Console.WriteLine(dr["dataset_code"].ToString() + "--" + Thread.CurrentThread.Name + "--" + DateTime.Now.ToString());
                var arrData = JObject.Parse(result);
                List<Dictionary<string, object>> lstDictColVal = new List<Dictionary<string, object>>();
                Dictionary<string, object> dictColVal = new Dictionary<string, object>();
                object objRefreshedAt = arrData["refreshed_at"] != null ? arrData["refreshed_at"] : arrData["updated_at"];
                foreach (var arr in arrData["data"])
                {
                    if (arr.Count() == 8)
                    {
                        dictColVal = new Dictionary<string, object>() {
                        { "Company_id", dr["company_id"] },
                        {"QuandlCompanyId", dr["id"] },
                        {"DATE",arr[0]},
                        {"OPEN",arr[1]},
                        {"High",arr[2]},
                        {"Low",arr[3]},
                        {"LAST",arr[4]},
                        {"CLOSE",arr[5]},
                        {"Total_Trade_Quantity",arr[6]},
                        {"Turnover",arr[7]},
                        {"Refreshed_At",objRefreshedAt}};


                        if (CheckDataExists("mr", "share", new Dictionary<string, string>() { { "company_id", dr["company_id"].ToString() }, { "date", dictColVal["DATE"].ToString() } }))
                            continue;
                        lstDictColVal.Add(dictColVal);
                        if (lstDictColVal.Count % 100 == 0)
                        {
                            InsertIntoTable("mr", "share", lstDictColVal, tableStructure, string.Empty, string.Empty);
                            lstDictColVal = new List<Dictionary<string, object>>();
                        }
                    }
                }
                if (lstDictColVal.Count > 0)
                {
                    InsertIntoTable("mr", "share", lstDictColVal, tableStructure, string.Empty, string.Empty);
                }
            }
        }
        static void InsertWeeklyReport(Dictionary<string, List<double>> dictWeeklyReport)
        {
            Dictionary<string, string> tableStructure = new Dictionary<string, string>(){
                        { "Company_id", "integer" },
                        {"YearWeek","integer"},
                        {"Avg","double"},
                        {"High","double"},
                        {"Low","double"},
                        {"PreviousAvg","double"},

                };
            List<Dictionary<string, object>> lstDictColVal = new List<Dictionary<string, object>>();
            Dictionary<string, object> dictColVal = new Dictionary<string, object>();

            //DataTable dtCode = GetDataFromTable("mr", "company", "id,company_id,dataset_code", new Dictionary<string, string>(), string.Empty);
            foreach (KeyValuePair<string, List<double>> kvp in dictWeeklyReport)
            {
                //count,sum,high,low,avg,prevAvg
                List<string> lstKey = kvp.Key.Split('_').ToList();
                dictColVal = new Dictionary<string, object>() {
                        { "Company_id", lstKey[1] },
                        {"YearWeek",lstKey[0]},
                        {"Avg",kvp.Value[4]},
                        {"High",kvp.Value[2]},
                        {"Low",kvp.Value[3]},
                        {"PreviousAvg",kvp.Value[5]}};


                //if (CheckDataExists("mr", "weeklyresult", new Dictionary<string, string>() { { "company_id", lstKey[1] }, { "YearWeek", lstKey[0] } }))
                //    continue;
                lstDictColVal.Add(dictColVal);
                if (lstDictColVal.Count % 100 == 0)
                {
                    InsertIntoTable("mr", "weeklyresult", lstDictColVal, tableStructure, string.Empty, string.Empty);
                    lstDictColVal = new List<Dictionary<string, object>>();
                }
            }
            if (lstDictColVal.Count > 0)
            {
                InsertIntoTable("mr", "weeklyresult", lstDictColVal, tableStructure, string.Empty, string.Empty);
            }
        }
        static void InsertIntoTable(string db, string table, List<Dictionary<string, object>> lstDictColVal, Dictionary<string, string> tableStructure, string primaryKeyCol, string mappedPrimaryKeyInJson)
        {
            StringBuilder query = new StringBuilder();
            string col = string.Empty;
            foreach (Dictionary<string, object> dict in lstDictColVal)
            {
                if (!string.IsNullOrWhiteSpace(primaryKeyCol) && CheckDataExists(db, table, primaryKeyCol, dict[mappedPrimaryKeyInJson].ToString()))
                    continue;
                if (string.IsNullOrWhiteSpace(col))
                {
                    col = String.Join(", ", dict.Keys.ToArray());
                    query.Append(string.Format(@"INSERT INTO {0}.{1} ({2}) values ", db, table, col));
                }
                string innerQuery = string.Empty;
                string val = string.Empty;
                foreach (KeyValuePair<string, object> kvp in dict)
                {
                    switch (tableStructure[kvp.Key].ToUpper())
                    {
                        case "INT":
                        case "INTEGER":
                        case "DOUBLE":
                        case "DECIMAL":
                        case "LONG":
                            val = kvp.Value == null || string.IsNullOrWhiteSpace(kvp.Value.ToString()) ? "null" : "'" + kvp.Value + "'";
                            break;
                        case "STRING":
                            val = kvp.Value == null || string.IsNullOrWhiteSpace(kvp.Value.ToString()) ? "null" : "'" + kvp.Value.ToString().Replace("\\", "\\\\").Replace("'", "''") + "'";
                            break;
                        case "BOOL":
                        case "BOOLEAN":
                            List<string> lstBool = new List<string>() { "TRUE", "1" };
                            object objVal = kvp.Value;
                            if (objVal != null)
                            {
                                objVal = lstBool.Contains(kvp.ToString().ToString()) ? "1" : "0";
                            }
                            val = objVal == null || string.IsNullOrWhiteSpace(objVal.ToString()) ? "null" : "'" + objVal + "'";
                            break;
                        case "ARRAY":
                            val = kvp.Value == null || string.IsNullOrWhiteSpace(kvp.Value.ToString()) ? "null" : "'" + String.Join(",", kvp.Value).Replace("\\", "\\\\").Replace("'", "''") + "'";
                            break;
                        case "DATETIME":
                        case "DATE":
                            val = kvp.Value == null || string.IsNullOrWhiteSpace(kvp.Value.ToString()) ? "null" : "'" + Convert.ToDateTime(kvp.Value).ToString("yyyy-MM-dd HH:mm:ss") + "'";
                            break;
                    }
                    innerQuery += val + ",";
                }
                innerQuery = "(" + innerQuery.TrimEnd(',') + "),";
                query.Append(innerQuery);
            }
            string nonQuery = query.ToString().TrimEnd(',');
            if (!string.IsNullOrWhiteSpace(nonQuery))
                Insert(nonQuery);
        }
        static void Insert(string nonQuery)
        {

            MySqlConnection con = new MySqlConnection(ConfigurationManager.ConnectionStrings["MRConnection"].ToString());
            string val = string.Empty;
            try
            {
                MySqlCommand cmd = new MySqlCommand(nonQuery, con);
                con.Open();
                cmd.ExecuteNonQuery();
                con.Close();
            }
            catch (Exception ex)
            {
            }
            finally
            {
                if (con.State == ConnectionState.Open)
                    con.Close();
            }


        }
        public static bool CheckDataExists(string db, string table, string column, string val)
        {
            MySqlConnection conn = new MySqlConnection();
            bool isDataExists = false;
            try
            {
                switch (db.ToLower())
                {
                    case "mr":
                        conn = new MySqlConnection(ConfigurationManager.ConnectionStrings["MRConnection"].ToString());
                        break;
                }
                string query = string.Format("select count(*) from {0} where {1}='{2}'", table, column, val);
                MySqlCommand cmd = new MySqlCommand(query, conn);
                cmd.Connection.Open();
                object objVal = cmd.ExecuteScalar();
                cmd.Connection.Close();
                if (objVal != null && Convert.ToInt32(objVal) > 0)
                    isDataExists = true;
            }
            catch (Exception ex)
            {
                //WebServiceCall.LogExceptions("Function : " + System.Reflection.MethodBase.GetCurrentMethod().Name + ", ErrorMessage : " + ex.Message, ex, ex.StackTrace);
            }
            finally
            {
                if (conn.State == ConnectionState.Open)
                    conn.Close();
            }
            return isDataExists;
        }
        public static bool CheckDataExists(string db, string table, Dictionary<string, string> dictColNVal)
        {
            MySqlConnection conn = new MySqlConnection();
            bool isDataExists = false;
            try
            {
                switch (db.ToLower())
                {
                    case "mr":
                        conn = new MySqlConnection(ConfigurationManager.ConnectionStrings["MRConnection"].ToString());
                        break;

                }
                StringBuilder query = new StringBuilder();
                query.Append(string.Format("select count(*) from {0} ", table));
                for (int i = 0; i < dictColNVal.Count; i++)
                {
                    if (i == 0)
                        query.Append(" where ");
                    query.Append(string.Format(" {0}='{1}' ", dictColNVal.ElementAt(i).Key, dictColNVal.ElementAt(i).Value));
                    if (!(i == dictColNVal.Count - 1))
                        query.Append(" and ");
                }

                MySqlCommand cmd = new MySqlCommand(query.ToString(), conn);
                cmd.Connection.Open();
                object objVal = cmd.ExecuteScalar();
                cmd.Connection.Close();
                if (objVal != null && Convert.ToInt32(objVal) > 0)
                    isDataExists = true;
            }
            catch (Exception ex)
            {
                //WebServiceCall.LogExceptions("Function : " + System.Reflection.MethodBase.GetCurrentMethod().Name + ", ErrorMessage : " + ex.Message, ex, ex.StackTrace);
            }
            finally
            {
                if (conn.State == ConnectionState.Open)
                    conn.Close();
            }
            return isDataExists;
        }
        public static DataTable GetDataFromTable(string db, string table, string columns, Dictionary<string, string> dictColNVal, string orderBy = "", int? limit = null, int? offset = null)
        {
            MySqlConnection conn = new MySqlConnection();
            DataTable dtData = new DataTable();
            try
            {
                switch (db.ToLower())
                {
                    case "mr":
                        conn = new MySqlConnection(ConfigurationManager.ConnectionStrings["MRConnection"].ToString());
                        break;
                }
                StringBuilder query = new StringBuilder();
                query.Append(string.Format("select {0} from {1} ", columns, table));
                for (int i = 0; i < dictColNVal.Count; i++)
                {
                    if (i == 0)
                        query.Append(" where ");
                    query.Append(string.Format(" {0}='{1}' ", dictColNVal.ElementAt(i).Key, dictColNVal.ElementAt(i).Value));
                    if (!(i == dictColNVal.Count - 1))
                        query.Append(" and ");
                }
                if (!string.IsNullOrWhiteSpace(orderBy))
                    query.Append(string.Format(" order by {0} ", orderBy));

                if (limit != null)
                    query.Append(string.Format(" limit {0} ", limit));
                if (offset != null)
                    query.Append(string.Format(" offset {0} ", offset));
                MySqlCommand cmd = new MySqlCommand(query.ToString(), conn);

                MySqlDataAdapter da = new MySqlDataAdapter(cmd);
                da.Fill(dtData);

            }
            catch (Exception ex)
            {
                //WebServiceCall.LogExceptions("Function : " + System.Reflection.MethodBase.GetCurrentMethod().Name + "table:" + table + ", ErrorMessage : " + ex.Message, ex, ex.StackTrace);
            }
            finally
            {
                if (conn.State == ConnectionState.Open)
                    conn.Close();
            }
            return dtData;
        }
        public static string GetShareDetailFromQuanDL(QuandlDownloadRequest request)
        {
            IQuandlConnection connection = new QuandlConnection();
            int counter = 0;
            string result = string.Empty;
            bool bIterate = true;
            while (bIterate)
            {
                counter++;
                try
                {
                    result = connection.Request(request);
                }
                catch
                { }
                if (!string.IsNullOrWhiteSpace(result) || counter > 99)
                    bIterate = false;
            }
            return result;

        }
        public static void GeTWeeklyReport()
        {
            DataTable dtCompany = GetDataFromTable("mr", "company", "*", new Dictionary<string, string>());
            DateTimeFormatInfo dfi = DateTimeFormatInfo.CurrentInfo;
            Calendar cal = dfi.Calendar;

            foreach (DataRow drCompany in dtCompany.Rows)
            {

                Dictionary<string, List<double>> dictWeekNumber_companyId_score = new Dictionary<string, List<double>>();
                string company_id = drCompany["company_id"].ToString();
                DataTable dtShare = GetDataFromTable("mr", "share", "*", new Dictionary<string, string>() { { "company_id", company_id } }, "date");
                string yearWeek = string.Empty;
                List<int> lstShare = new List<int>();

                foreach (DataRow drShare in dtShare.Rows)
                {
                    DateTime dt = new DateTime();
                    if (DateTime.TryParse(drShare["date"].ToString(), out dt))
                    {
                        int weekNumber = cal.GetWeekOfYear(dt, dfi.CalendarWeekRule, dfi.FirstDayOfWeek);
                        double open = Convert.ToDouble(drShare["open"]);
                        double close = drShare["close"] == DBNull.Value ? open : Convert.ToDouble(drShare["close"]);
                        double high = drShare["high"] == DBNull.Value ? open : Convert.ToDouble(drShare["high"]);
                        double low = drShare["low"] == DBNull.Value ? open : Convert.ToDouble(drShare["low"]);
                        yearWeek = weekNumber < 10 ? dt.Year.ToString() + "0" + weekNumber : dt.Year.ToString() + weekNumber;

                        string key = yearWeek + "_" + company_id;
                        if (!dictWeekNumber_companyId_score.ContainsKey(key))
                        {
                            //count,sum,high,low,avg,prevAvg
                            List<Double> lst;
                            if (dictWeekNumber_companyId_score.Keys.Count == 0)
                                lst = new List<double>() { 0, 0, 0, 0, 0, 0 };
                            else
                                lst = new List<double>() { 0, 0, 0, 0, 0, dictWeekNumber_companyId_score.Values.Last()[4] };


                            dictWeekNumber_companyId_score.Add(key, lst);
                        }
                        dictWeekNumber_companyId_score[key][0] += 1;
                        dictWeekNumber_companyId_score[key][1] += close;
                        dictWeekNumber_companyId_score[key][2] = Math.Max(dictWeekNumber_companyId_score[key][2], high);
                        dictWeekNumber_companyId_score[key][3] = dictWeekNumber_companyId_score[key][3] == 0 ? low : Math.Min(dictWeekNumber_companyId_score[key][3], low);
                        dictWeekNumber_companyId_score[key][4] = dictWeekNumber_companyId_score[key][1] / dictWeekNumber_companyId_score[key][0];
                    }
                }
                InsertWeeklyReport(dictWeekNumber_companyId_score);
            }

        }
    }
    public class Company
    {
        int id { get; set; }
        string dataset_code { get; set; }
        string database_code { get; set; }
        string name { get; set; }
        string description { get; set; }
        string refreshed_at { get; set; }
        string newest_available_date { get; set; }
        string oldest_available_date { get; set; }
        string column_names { get; set; }
        string frequency { get; set; }
        string type { get; set; }
        string premium { get; set; }
        int database_id { get; set; }
    }
}
