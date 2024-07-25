/*
****************************************************************************
*  Copyright (c) 2024,  Skyline Communications NV  All Rights Reserved.    *
****************************************************************************

By using this script, you expressly agree with the usage terms and
conditions set out below.
This script and all related materials are protected by copyrights and
other intellectual property rights that exclusively belong
to Skyline Communications.

A user license granted for this script is strictly for personal use only.
This script may not be used in any way by anyone without the prior
written consent of Skyline Communications. Any sublicensing of this
script is forbidden.

Any modifications to this script by the user are only allowed for
personal use and within the intended purpose of the script,
and will remain the sole responsibility of the user.
Skyline Communications will not be responsible for any damages or
malfunctions whatsoever of the script resulting from a modification
or adaptation by the user.

The content of this script is confidential information.
The user hereby agrees to keep this confidential information strictly
secret and confidential and not to disclose or reveal it, in whole
or in part, directly or indirectly to any person, entity, organization
or administration without the prior written consent of
Skyline Communications.

Any inquiries can be addressed to:

	Skyline Communications NV
	Ambachtenstraat 33
	B-8870 Izegem
	Belgium
	Tel.	: +32 51 31 35 69
	Fax.	: +32 51 31 01 29
	E-mail	: info@skyline.be
	Web		: www.skyline.be
	Contact	: Ben Vandenberghe

****************************************************************************
Revision History:

DATE		VERSION		AUTHOR			COMMENTS

07/04/2023	1.0.0.1		RGE, Skyline	Initial version

25/01/2024	1.0.0.2		EVA, Skyline	Adapted original script to work on the 10.3.9 DataMiner version
****************************************************************************
*/

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;

using CsvHelper;
using CsvHelper.Configuration;
using CsvHelper.TypeConversion;

using Skyline.DataMiner.Analytics.GenericInterface;

[GQIMetaData(Name = "Csv file")]
public class CSVDataSource : IGQIDataSource, IGQIInputArguments
{
	private const string CSV_ROOT_PATH = @"C:\Skyline DataMiner\Documents";

	private const string FILE_ARGUMENT_NAME = "File";

	private readonly DateTimeConverter _dateTimeConverter;

	private readonly GQIStringArgument _delimiterArgument;

	private string _csvFilePath;

	private string _delimiter;

	private HeaderInfo _headerInfo;

	private GQIRow[] _rows;

	public CSVDataSource()
	{
		_dateTimeConverter = new DateTimeConverter();

		_delimiterArgument = new GQIStringArgument("Delimiter")
		{
			DefaultValue = ",",
		};
	}

	public GQIColumn[] GetColumns()
	{
		return _headerInfo.Columns;
	}

	public GQIArgument[] GetInputArguments()
	{
		var csvFileOptions = GetCsvFileOptions();
		if (csvFileOptions.Length == 0)
			throw new GenIfException($"No csv files available in '{CSV_ROOT_PATH}'.");

		var fileArgument = new GQIStringDropdownArgument(FILE_ARGUMENT_NAME, csvFileOptions)
		{
			IsRequired = true,
		};

		return new GQIArgument[]
		{
			fileArgument,
			_delimiterArgument,
		};
	}

	public GQIPage GetNextPage(GetNextPageInputArgs args)
	{
		return new GQIPage(_rows);
	}

	public OnArgumentsProcessedOutputArgs OnArgumentsProcessed(OnArgumentsProcessedInputArgs args)
	{
		_headerInfo = default;
		_rows = null;

		var fileArgument = new GQIStringArgument(FILE_ARGUMENT_NAME);
		var csvFileOption = args.GetArgumentValue(fileArgument);

		if (string.IsNullOrEmpty(csvFileOption))
			throw new GenIfException("Missing csv file.");

		var relativeFileName = csvFileOption.Replace("/", @"\");
		_csvFilePath = $@"{CSV_ROOT_PATH}\{relativeFileName}.csv";

		if (!File.Exists(_csvFilePath))
			throw new GenIfException($"Csv file does not exist: {_csvFilePath}");

		_delimiter = args.GetArgumentValue(_delimiterArgument);
		if (string.IsNullOrEmpty(_delimiter))
			_delimiter = ",";

		ReadCsvFile();

		return default;
	}

	private static GQIColumn GetColumn(string name, string type)
	{
		switch (type)
		{
			case "bool": return new GQIBooleanColumn(name);
			case "datetime": return new GQIDateTimeColumn(name);
			case "double": return new GQIDoubleColumn(name);
			case "int": return new GQIIntColumn(name);
			default: return new GQIStringColumn(name);
		}
	}

	private static (string name, string type) GetColumnInfo(string head)
	{
		var separatorIndex = head.IndexOf("::");
		if (separatorIndex == -1)
			return (head, "string");

		var name = head.Substring(0, separatorIndex);
		var type = head.Substring(separatorIndex + 2);
		return (name, type);
	}

	private static string[] GetCsvFileOptions()
	{
		if (!Directory.Exists(CSV_ROOT_PATH))
			throw new GenIfException($"Csv file root path does not exist: {CSV_ROOT_PATH}");

		return Directory.EnumerateFiles(CSV_ROOT_PATH, "*.csv", SearchOption.AllDirectories)
			.Select(fileName =>
			{
				var relativeFileName = fileName
					.AsSpan()
					.Slice(CSV_ROOT_PATH.Length + 1, fileName.Length - CSV_ROOT_PATH.Length - 5)
					.ToString();
				return relativeFileName.Replace(@"\", "/");
			})
			.ToArray();
	}

	private static HeaderInfo GetHeaderInfo(string[] header)
	{
		var keyIndex = -1;
		var columns = new List<GQIColumn>();

		for (int i = 0; i < header.Length; i++)
		{
			var columnInfo = GetColumnInfo(header[i]);

			if (columnInfo.type == "key")
			{
				if (keyIndex != -1)
					throw new GenIfException($"Duplicate key definition at column {keyIndex} and column {i}.");
				keyIndex = i;
			}

			var column = GetColumn(columnInfo.name, columnInfo.type);
			columns.Add(column);
		}

		return new HeaderInfo(keyIndex, columns.ToArray());
	}

	private GQICell GetCell(CsvReader reader, int index, Type type)
	{
		if (type == typeof(DateTime))
		{
			var dateTime = reader.GetField<DateTime>(index, _dateTimeConverter);
			return new GQICell { Value = dateTime };
		}

		var value = reader.GetField(type, index);
		return new GQICell { Value = value };
	}

	private void ReadCsvFile()
	{
		var config = new CsvConfiguration(CultureInfo.InvariantCulture)
		{
			Delimiter = _delimiter,
		};

		using (var fileStream = new FileStream(_csvFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete))
		using (var streamReader = new StreamReader(fileStream))
		using (var csvReader = new CsvReader(streamReader, config))
		{
			csvReader.Read();
			csvReader.ReadHeader();
			_headerInfo = GetHeaderInfo(csvReader.HeaderRecord);
			_rows = ReadRows(csvReader);
		}
	}

	private GQIRow ReadRow(CsvReader reader, Type[] columnTypes)
	{
		var cells = columnTypes.Select((type, index) => GetCell(reader, index, type));
		return new GQIRow(cells.ToArray());
	}

	private GQIRow[] ReadRows(CsvReader csvReader)
	{
		var columnTypes = _headerInfo.GetColumnTypes();
		return ReadRows(csvReader, columnTypes);
	}

	private GQIRow[] ReadRows(CsvReader reader, Type[] columnTypes)
	{
		var rows = new List<GQIRow>();
		while (reader.Read())
		{
			var row = ReadRow(reader, columnTypes);
			rows.Add(row);
		}

		return rows.ToArray();
	}

	private sealed class DateTimeConverter : ITypeConverter
	{
		public object ConvertFromString(string text, IReaderRow row, MemberMapData memberMapData)
		{
			try
			{
				DateTime dateTime = DateTime.Parse(text, CultureInfo.InvariantCulture);

				return DateTime.SpecifyKind(dateTime, DateTimeKind.Utc);
			}
			catch (FormatException)
			{
				throw new GenIfException(text);
			}
		}

		public string ConvertToString(object value, IWriterRow row, MemberMapData memberMapData)
		{
			return value.ToString();
		}
	}

	private sealed class HeaderInfo
	{
		public HeaderInfo(int keyIndex, GQIColumn[] columns)
		{
			KeyIndex = keyIndex;
			Columns = columns;
		}

		public GQIColumn[] Columns { get; }

		public int KeyIndex { get; }

		public Type[] GetColumnTypes()
		{
			return Columns.Select(column => GetColumnType(column.Type)).ToArray();
		}

		private static Type GetColumnType(GQIColumnType type)
		{
			switch (type)
			{
				case GQIColumnType.Boolean: return typeof(bool);
				case GQIColumnType.DateTime: return typeof(DateTime);
				case GQIColumnType.Double: return typeof(double);
				case GQIColumnType.Int: return typeof(int);
				default: return typeof(string);
			}
		}
	}
}