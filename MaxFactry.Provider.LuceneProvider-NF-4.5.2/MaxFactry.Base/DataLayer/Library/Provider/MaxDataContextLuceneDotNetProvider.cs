// <copyright file="MaxDataContextLuceneDotNetProvider.cs" company="Lakstins Family, LLC">
// Copyright (c) Brian A. Lakstins (http://www.lakstins.com/brian/)
// </copyright>

#region License
// <license>
// This software is provided 'as-is', without any express or implied warranty. In no 
// event will the author be held liable for any damages arising from the use of this 
// software.
//  
// Permission is granted to anyone to use this software for any purpose, including 
// commercial applications, and to alter it and redistribute it freely, subject to the 
// following restrictions:
// 
// 1. The origin of this software must not be misrepresented; you must not claim that 
// you wrote the original software. If you use this software in a product, an 
// acknowledgment (see the following) in the product documentation is required.
// 
// Portions Copyright (c) Brian A. Lakstins (http://www.lakstins.com/brian/)
// 
// 2. Altered source versions must be plainly marked as such, and must not be 
// misrepresented as being the original software.
// 
// 3. This notice may not be removed or altered from any source distribution.
// </license>
#endregion

#region Change Log
// <changelog>
// <change date="5/19/2014" author="Brian A. Lakstins" description="Initial Release">
// <change date="6/17/2014" author="Brian A. Lakstins" description="Update for base method rename.">
// <change date="6/26/2014" author="Brian A. Lakstins" description="Update for addition of StorageKey.">
// <change date="8/21/2014" author="Brian A. Lakstins" description="Added stream methods.">
// <change date="9/26/2014" author="Brian A. Lakstins" description="Update to run index optimization only when a certain amount of time or writes have occurred.">
// <change date="11/10/2014" author="Brian A. Lakstins" description="Updates for changes to core.">
// <change date="11/27/2014" author="Brian A. Lakstins" description="Fix to handle null data.">
// <change date="12/3/2014" author="Brian A. Lakstins" description="Update to handle boolean exact matches as part of key, handle single terms, handle multiple terms, handle exact quoted terms.">
// <change date="7/4/2016" author="Brian A. Lakstins" description="Updated to access provider configuration using base provider methods.">
// <change date="11/30/2018" author="Brian A. Lakstins" description="Updated for change to base.">
// <change date="6/15/2020" author="Brian A. Lakstins" description="Updated for change to base.">
// <change date="11/6/2020" author="Brian A. Lakstins" description="Add indexing of long strings and optimize call to get field type info">
// <change date="1/11/2021" author="Brian A. Lakstins" description="Fix return of number of records changed">
// <change date="1/17/2021" author="Brian A. Lakstins" description="Fix return of number of records changed - again">
// </changelog>
#endregion

namespace MaxFactry.Base.DataLayer.Library.Provider
{
    using System;
    using System.IO;
    using System.Text;
    using Lucene.Net.Analysis;
    using Lucene.Net.Documents;
    using Lucene.Net.Index;
    using Lucene.Net.QueryParsers;
    using Lucene.Net.Search;
    using Lucene.Net.Store;
    using MaxFactry.Core;
    using MaxFactry.Base.DataLayer;

    /// <summary>
    /// Provides session services using MaxFactryLibrary.
    /// </summary>
    public class MaxDataContextLuceneDotNetProvider : MaxProvider, IMaxDataContextProvider
    {
        /// <summary>
        /// Number of writes made since the application started.
        /// </summary>
        private static int _nWriteCount = 0;

        /// <summary>
        /// Last time optimization was run.
        /// </summary>
        private static DateTime _nLastOptimize = DateTime.Now;

        /// <summary>
        /// Base file system directory to use to store data
        /// </summary>
        private string _sBaseDirectory = string.Empty;

        /// <summary>
        /// List of tables that have been created.
        /// </summary>
        private MaxIndex _oTableList = new MaxIndex();

        /// <summary>
        /// Internal storage of search index (IndexSearcher)
        /// </summary>
        private MaxIndex _oSearcherIndex = new MaxIndex();

        /// <summary>
        /// Initializes the provider
        /// </summary>
        /// <param name="lsName">Name of the provider</param>
        /// <param name="loConfig">Configuration information</param>
        public override void Initialize(string lsName, MaxIndex loConfig)
        {

            base.Initialize(lsName, loConfig);
            string lsFolder = this.GetConfigValue(loConfig, "LuceneFolder") as string;
            if (null != lsFolder)
            {
                this._sBaseDirectory = lsFolder;
            }
        }

        /// <summary>
        /// Selects all data from the data storage name for the specified type.
        /// </summary>
        /// <param name="lsDataStorageName">Name of the data storage (table name).</param>
        /// <param name="laFields">list of fields to return from select</param>
        /// <returns>List of data elements with a base data model.</returns>
        public virtual MaxDataList SelectAll(string lsDataStorageName, params string[] laFields)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Selects data from the database.
        /// </summary>
        /// <param name="loData">Element with data used in the filter.</param>
        /// <param name="loDataQuery">Query information to filter results.</param>
        /// <param name="lnPageIndex">Page to return.</param>
        /// <param name="lnPageSize">Items per page.</param>
        /// <param name="lnTotal">Total items found.</param>
        /// <param name="laFields">list of fields to return from select.</param>
        /// <returns>List of data from select.</returns>
        public MaxDataList Select(MaxData loData, MaxDataQuery loDataQuery, int lnPageIndex, int lnPageSize, string lsSort, out int lnTotal, params string[] laFields)
        {
            System.Diagnostics.Stopwatch loWatch = System.Diagnostics.Stopwatch.StartNew();
            MaxFactry.Core.MaxLogLibrary.Log(MaxFactry.Core.MaxEnumGroup.LogDebug, "Select([" + loData.DataModel.GetType().ToString() + "]) start", "MaxLuceneDotNetDataContextProvider");

            int lnRows = 0;
            int lnStart = 0;
            int lnEnd = int.MaxValue;
            if (lnPageSize > 0 && lnPageIndex > 0)
            {
                lnStart = (lnPageIndex - 1) * lnPageSize;
                lnEnd = lnStart + lnPageSize;
            }

            MaxDataList loDataList = new MaxDataList(loData.DataModel);
            lnTotal = 0;

            if (System.IO.File.Exists(System.IO.Path.Combine(this.GetDirectory(loData), "segments.gen")))
            {
                IndexSearcher loSearcher = this.GetSearcher(loData);
                Query loQuery = this.GetQuery(loData, loDataQuery);
                TopDocs loDocList = loSearcher.Search(loQuery, lnEnd);
                MaxFactry.Core.MaxLogLibrary.Log(MaxFactry.Core.MaxEnumGroup.LogInfo, "Search [" + loQuery.ToString() + "] TotalHits [" + loDocList.TotalHits.ToString() + "] in [" + loWatch.ElapsedMilliseconds.ToString() + "] milliseconds.", "MaxLuceneDotNetDataContextProvider");
                lnTotal = loDocList.TotalHits;
                string[] laKey = loData.DataModel.GetKeyList();

                for (int lnR = lnStart; lnR < loDocList.ScoreDocs.Length && lnR < lnEnd; lnR++)
                {
                    ScoreDoc loDoc = loDocList.ScoreDocs[lnR];
                    MaxData loDataNew = new MaxData(loData.DataModel);
                    for (int lnD = 0; lnD < laKey.Length; lnD++)
                    {
                        string lsKey = laKey[lnD];
                        string lsValue = loSearcher.Doc(loDoc.Doc).Get(lsKey);
                        if (null != lsValue)
                        {
                            Type loValueType = loData.DataModel.GetValueType(lsKey);
                            if (loValueType == typeof(Guid))
                            {
                                loDataNew.Set(lsKey, Guid.Parse(lsValue));
                            }
                            else if (loValueType == typeof(bool))
                            {
                                loDataNew.Set(lsKey, bool.Parse(lsValue));
                            }
                            else if (loValueType == typeof(DateTime))
                            {
                                loDataNew.Set(lsKey, DateTime.Parse(lsValue));
                            }
                            else if (loValueType == typeof(double))
                            {
                                if (lsValue.Equals("-1.79769313486232E+308"))
                                {
                                    loDataNew.Set(lsKey, double.MinValue);
                                }
                                else
                                {
                                    loDataNew.Set(lsKey, double.Parse(lsValue));
                                }
                            }
                            else if (loValueType == typeof(int))
                            {
                                if (lsValue.Equals("-2147483648"))
                                {
                                    loDataNew.Set(lsKey, int.MinValue);
                                }
                                else
                                {
                                    loDataNew.Set(lsKey, int.Parse(lsValue));
                                }
                            }
                            else if (loValueType == typeof(long))
                            {
                                if (lsValue.Equals("-9223372036854775808"))
                                {
                                    loDataNew.Set(lsKey, long.MinValue);
                                }
                                else
                                {
                                    loDataNew.Set(lsKey, long.Parse(lsValue));
                                } 
                            }
                            else if (loValueType == typeof(string) ||
                                loValueType == typeof(MaxShortString))
                            {
                                loDataNew.Set(laKey[lnD], lsValue);
                            }
                        }
                    }

                    loDataNew.Set("MaxSelectScore", Convert.ToDouble(loDoc.Score));
                    loDataList.Add(loDataNew);
                    lnRows++;
                }

                MaxFactry.Core.MaxLogLibrary.Log(MaxFactry.Core.MaxEnumGroup.LogDebug, "Search([" + loData.DataModel.GetType().ToString() + "]) execute query after [" + lnRows.ToString() + "]", "MaxLuceneDotNetDataContextProvider");
                lnTotal = lnRows;
                MaxFactry.Core.MaxLogLibrary.Log(MaxFactry.Core.MaxEnumGroup.LogDebug, "Search([" + loData.DataModel.GetType().ToString() + "]) end", "MaxLuceneDotNetDataContextProvider");
                MaxFactry.Core.MaxLogLibrary.Log(MaxFactry.Core.MaxEnumGroup.LogInfo, "Search [" + loQuery.ToString() + "] returned [" + lnRows.ToString() + "] in [" + loWatch.ElapsedMilliseconds.ToString() + "] milliseconds.", "MaxLuceneDotNetDataContextProvider");
            }

            return loDataList;
        }

        /// <summary>
        /// Gets the number of records that match the filter.
        /// </summary>
        /// <param name="loData">Element with data used in the filter.</param>
        /// <param name="loDataQuery">Query information to filter results.</param>
        /// <returns>number of records that match.</returns>
        public int SelectCount(MaxData loData, MaxDataQuery loDataQuery)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Inserts a new data element.
        /// </summary>
        /// <param name="loDataList">The list of data objects to insert.</param>
        /// <returns>The data that was inserted.</returns>
        public int Insert(MaxDataList loDataList)
        {
            int lnR = 0;
            for (int lnD = 0; lnD < loDataList.Count; lnD++)
            {
                MaxData loData = loDataList[lnD];
                FSDirectory loDirectory = FSDirectory.Open(this.GetDirectory(loData));
                try
                {
                    Analyzer loAnalyzer = this.GetAnalyzer();
                    IndexWriter loWriter = new IndexWriter(loDirectory, loAnalyzer, IndexWriter.MaxFieldLength.UNLIMITED);
                    try
                    {
                        Document loDocument = this.GetDynamicDocument(loData);
                        loWriter.AddDocument(loDocument);
                        loWriter.Commit();
                        _nWriteCount++;
                        lnR++;
                        if (_nWriteCount % 100 == 0 || _nLastOptimize.AddMinutes(5) < DateTime.Now)
                        {
                            loWriter.Optimize();
                            _nLastOptimize = DateTime.Now;
                        }
                    }
                    finally
                    {
                        loWriter.Dispose();
                    }
                }
                finally
                {
                    loDirectory.Dispose();
                    this.CloseSearcher(loData);
                }
            }

            return lnR;
        }

        /// <summary>
        /// Updates an existing data element.
        /// </summary>
        /// <param name="loDataList">The list of data objects to insert.</param>
        /// <returns>The data that was updated.</returns>
        public int Update(MaxDataList loDataList)
        {
            int lnR = 0;
            for (int lnD = 0; lnD < loDataList.Count; lnD++)
            {
                MaxData loData = loDataList[lnD];
                Analyzer loAnalyzer = this.GetAnalyzer();
                Query loQuery = this.GetQuery(loData, new MaxDataQuery());
                FSDirectory loDirectory = FSDirectory.Open(this.GetDirectory(loData));
                Document loDocument = this.GetDynamicDocument(loData);
                IndexWriter loWriter = new IndexWriter(loDirectory, loAnalyzer, IndexWriter.MaxFieldLength.UNLIMITED);
                try
                {
                    loWriter.DeleteDocuments(loQuery);
                    loWriter.AddDocument(loDocument);
                    loWriter.Commit();
                    _nWriteCount++;
                    lnR++;
                    if (_nWriteCount % 100 == 0 || _nLastOptimize.AddMinutes(5) < DateTime.Now)
                    {
                        loWriter.Optimize();
                        _nLastOptimize = DateTime.Now;
                    }
                }
                finally
                {
                    loWriter.Dispose();
                    loDirectory.Dispose();
                    this.CloseSearcher(loData);
                }
            }

            return lnR;
        }

        /// <summary>
        /// Deletes an existing data element.
        /// </summary>
        /// <param name="loDataList">The list of data objects to insert.</param>
        /// <returns>true if deleted.</returns>
        public int Delete(MaxDataList loDataList)
        {
            int lnR = 0;
            for (int lnD = 0; lnD < loDataList.Count; lnD++)
            {
                MaxData loData = loDataList[lnD];
                Analyzer loAnalyzer = this.GetAnalyzer();
                Query loQuery = this.GetQuery(loData, new MaxDataQuery());
                FSDirectory loDirectory = FSDirectory.Open(this.GetDirectory(loData));
                IndexWriter loWriter = new IndexWriter(loDirectory, loAnalyzer, IndexWriter.MaxFieldLength.UNLIMITED);
                try
                {
                    loWriter.DeleteDocuments(loQuery);
                    loWriter.Commit();
                    _nWriteCount++;
                    lnR++;
                    if (_nWriteCount % 100 == 0 || _nLastOptimize.AddMinutes(5) < DateTime.Now)
                    {
                        loWriter.Optimize();
                        _nLastOptimize = DateTime.Now;
                    }
                }
                finally
                {
                    loWriter.Dispose();
                    loDirectory.Dispose();
                    this.CloseSearcher(loData);
                }
            }

            return lnR;
        }

        /// <summary>
        /// Writes stream data to storage.
        /// </summary>
        /// <param name="loData">The data index for the object</param>
        /// <param name="lsKey">Data element name to write</param>
        /// <returns>Number of bytes written to storage.</returns>
        public virtual bool StreamSave(MaxData loData, string lsKey)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Opens stream data in storage
        /// </summary>
        /// <param name="loData">The data index for the object</param>
        /// <param name="lsKey">Data element name to write</param>
        /// <returns>Stream that was opened.</returns>
        public virtual Stream StreamOpen(MaxData loData, string lsKey)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Removes stream from storage.
        /// </summary>
        /// <param name="loData">The data index for the object</param>
        /// <param name="lsKey">Data element name to remove</param>
        /// <returns>true if successful.</returns>
        public virtual bool StreamDelete(MaxData loData, string lsKey)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Gets the Url of a saved stream.
        /// </summary>
        /// <param name="loData">The data index for the object</param>
        /// <param name="lsKey">Data element name</param>
        /// <returns>Url of stream if one can be provided.</returns>
        public string GetStreamUrl(MaxData loData, string lsKey)
        {
            return string.Empty;
        }

        /// <summary>
        /// Gets a dynamic table entity based on the data.
        /// </summary>
        /// <param name="loData">The MaxData object containing the data.</param>
        /// <returns>A dynamic table entity.</returns>
        protected Document GetDynamicDocument(MaxData loData)
        {
            Document loDocument = new Document();
            string[] laKey = loData.DataModel.GetKeyList();
            for (int lnK = 0; lnK < laKey.Length; lnK++)
            {
                string lsKey = laKey[lnK];
                Type loValueType = loData.DataModel.GetValueType(lsKey);
                Field.Index loAnalyze = Field.Index.NOT_ANALYZED;
                Field.TermVector loVector = Field.TermVector.NO;
                if (loValueType == typeof(MaxShortString))
                {
                    loVector = Field.TermVector.YES;
                }
                else if (loValueType == typeof(string) || loValueType == typeof(MaxLongString))
                {
                    loAnalyze = Field.Index.ANALYZED;
                    loVector = Field.TermVector.YES;
                }
                else if (loValueType == typeof(bool))
                {
                    loAnalyze = Field.Index.ANALYZED;
                }

                AbstractField loField = null;
                if (loValueType == typeof(int))
                {
                    loField = new NumericField(lsKey, Field.Store.YES, true);
                    ((NumericField)loField).SetIntValue(MaxConvertLibrary.ConvertToInt(loData.DataModel.GetType(), loData.Get(lsKey)));
                }
                else if (loValueType == typeof(long))
                {
                    loField = new NumericField(lsKey, Field.Store.YES, true);
                    ((NumericField)loField).SetLongValue(MaxConvertLibrary.ConvertToLong(loData.DataModel.GetType(), loData.Get(lsKey)));
                }
                else if (loValueType == typeof(double))
                {
                    loField = new NumericField(lsKey, Field.Store.YES, true);
                    ((NumericField)loField).SetDoubleValue(MaxConvertLibrary.ConvertToDouble(loData.DataModel.GetType(), loData.Get(lsKey)));
                }
                else 
                {
                    object loValue = loData.Get(lsKey);
                    if (null != loValue)
                    {
                        loField = new Field(lsKey, loValue.ToString(), Field.Store.YES, loAnalyze, loVector);
                    }
                }

                if (null != loField)
                {
                    string lsBoost = loData.DataModel.GetPropertyAttribute(lsKey, "SearchBoost");
                    if (!string.IsNullOrEmpty(lsBoost))
                    {
                        float lnBoost = 1.0F;
                        if (float.TryParse(lsBoost, out lnBoost))
                        {
                            loField.Boost = lnBoost;
                        }
                    }

                    loDocument.Add(loField);
                }
            }

            return loDocument;
        }

        /// <summary>
        /// Gets the directory object based on the data.
        /// </summary>
        /// <param name="loData">The data to store.</param>
        /// <returns>Lucene directory.</returns>
        protected IndexSearcher GetSearcher(MaxData loData)
        {
            string lsDirectory = this.GetDirectory(loData);

            if (this._oSearcherIndex.Contains(lsDirectory) && this._oSearcherIndex[lsDirectory] is IndexSearcher)
            {
                return (IndexSearcher)this._oSearcherIndex[lsDirectory];
            }

            FSDirectory loDirectory = FSDirectory.Open(lsDirectory);
            IndexSearcher loSearcher = new IndexSearcher(loDirectory);
            this._oSearcherIndex.Add(lsDirectory, loSearcher);
            return loSearcher;
        }

        /// <summary>
        /// Gets the directory object based on the data.
        /// </summary>
        /// <param name="loData">The data to store.</param>
        protected void CloseSearcher(MaxData loData)
        {
            string lsDirectory = this.GetDirectory(loData);

            if (this._oSearcherIndex.Contains(lsDirectory) && this._oSearcherIndex[lsDirectory] is IndexSearcher)
            {
                IndexSearcher loSearcher = (IndexSearcher)this._oSearcherIndex[lsDirectory];
                this._oSearcherIndex.Remove(lsDirectory);
                loSearcher.Dispose();
            }
        }

        /// <summary>
        /// Gets the directory object based on the data.
        /// </summary>
        /// <param name="loData">The data to store.</param>
        /// <returns>Lucene directory.</returns>
        protected string GetDirectory(MaxData loData)
        {
            string lsDirectory = this._sBaseDirectory;
            lsDirectory = System.IO.Path.Combine(lsDirectory, loData.Get(loData.DataModel.StorageKey).ToString());
            lsDirectory = System.IO.Path.Combine(lsDirectory, loData.DataModel.DataStorageName);
            if (!System.IO.Directory.Exists(lsDirectory))
            {
                System.IO.Directory.CreateDirectory(lsDirectory);
            }

            return lsDirectory;
        }

        /// <summary>
        /// Gets the analyzer to be used.
        /// </summary>
        /// <returns>The Lucene analyzer.</returns>
        protected Analyzer GetAnalyzer()
        {
            return new Lucene.Net.Analysis.Standard.StandardAnalyzer(Lucene.Net.Util.Version.LUCENE_30);
        }

        /// <summary>
        /// Gets the query based on the data and data query information.
        /// </summary>
        /// <param name="loData">The data for the query.</param>
        /// <param name="loDataQuery">The data query information for the query.</param>
        /// <returns>The Lucene query.</returns>
        protected Query GetQuery(MaxData loData, MaxDataQuery loDataQuery)
        {
            MaxLogLibrary.Log(MaxEnumGroup.LogDebug, "GetQuery start", "MaxLuceneDotNetDataContextProvider");
            BooleanQuery loQuery = new BooleanQuery();
            string[] laKey = loData.DataModel.GetKeyList();

            string[] laKeyList = loData.DataModel.GetKeyList();

            for (int lnK = 0; lnK < laKeyList.Length; lnK++)
            {
                string lsKey = laKeyList[lnK];
                if (null != loData.Get(lsKey))
                {
                    bool lbIsQueryKey = loData.DataModel.GetPropertyAttributeSetting(lsKey, "IsPrimaryKey");
                    object loObject = loData.Get(lsKey + "-IsQueryKey");
                    if (null != loObject && loObject is bool)
                    {
                        lbIsQueryKey = (bool)loObject;
                    }

                    if (lbIsQueryKey)
                    {
                        if (null != loData.Get(lsKey))
                        {
                            string lsValue = loData.Get(lsKey).ToString().ToLowerInvariant();
                            TermQuery loTermQuery = new TermQuery(new Term(lsKey, lsValue));
                            loQuery.Add(loTermQuery, Occur.MUST);
                            MaxLogLibrary.Log(MaxEnumGroup.LogDebug, "GetQuery TermQuery=[" + lsKey + "][" + lsValue + "]", "MaxLuceneDotNetDataContextProvider");
                        }
                    }
                }
            }

            object[] laDataQuery = loDataQuery.GetQuery();
            if (laDataQuery.Length > 0)
            {
                string lsDataQuery = string.Empty;
                for (int lnDQ = 0; lnDQ < laDataQuery.Length; lnDQ++)
                {
                    object loStatement = laDataQuery[lnDQ];
                    if (loStatement is char)
                    {
                        // Group Start or end characters
                        lsDataQuery += (char)loStatement;
                    }
                    else if (loStatement is string)
                    {
                        // Combination operators (and, or)
                        lsDataQuery += " " + ((string)loStatement).ToUpper() + " ";
                    }
                    else if (loStatement is MaxDataFilter)
                    {
                        MaxDataFilter loDataFilter = (MaxDataFilter)loStatement;
                        lsDataQuery += this.GetFilterCondition(loDataFilter.Name, loDataFilter.Operator, loDataFilter.Value, loData.DataModel);
                    }
                }

                QueryParser loParser = new QueryParser(Lucene.Net.Util.Version.LUCENE_30, "id", this.GetAnalyzer());
                MaxLogLibrary.Log(MaxEnumGroup.LogDebug, "GetQuery lsDataQuery=[" + lsDataQuery + "]", "MaxLuceneDotNetDataContextProvider");
                Query loDataQueryParsed = loParser.Parse(lsDataQuery);
                loQuery.Add(loDataQueryParsed, Occur.MUST);
            }

            MaxLogLibrary.Log(MaxEnumGroup.LogDebug, "GetQuery end [" + loQuery.ToString() + "]", "MaxLuceneDotNetDataContextProvider");
            return loQuery;
        }

        /// <summary>
        /// Gets the filter element.
        /// <see cref="http://lucene.apache.org/core/2_9_4/queryparsersyntax.html"/>
        /// <see cref="https://today.java.net/pub/a/today/2003/11/07/QueryParserRules.html"/>
        /// </summary>
        /// <param name="lsName">Column name in the table.</param>
        /// <param name="lsOperator">Operator to compare the value.</param>
        /// <param name="loValue">The value to compare.</param>
        /// <param name="loDataModel">The definition of the data.</param>
        /// <returns>String filter element.</returns>
        protected string GetFilterCondition(string lsName, string lsOperator, object loValue, MaxDataModel loDataModel)
        {
            string lsOperation = ":";
            string lsValue = loValue.ToString();
            string lsR = string.Empty;
            if (lsValue.StartsWith("\"") && lsValue.EndsWith("\"") && lsValue.Length > 2)
            {
                lsR = string.Format("{0}{1}{2} ", lsName, lsOperation, "\"" + QueryParser.Escape(lsValue.Substring(1, lsValue.Length - 2)) + "\"");
            }
            else
            {
                string[] laValue = lsValue.Split(new string[] { " " }, StringSplitOptions.RemoveEmptyEntries);
                foreach (string lsVal in laValue)
                {
                    if (!lsR.Equals(string.Empty))
                    {
                        lsR += " OR ";
                    }

                    lsR += string.Format("{0}{1}{2} ", lsName, lsOperation, QueryParser.Escape(lsVal));
                }

                lsR = "(" + lsR + ")";
            }

            return lsR;
        }
    }
}
