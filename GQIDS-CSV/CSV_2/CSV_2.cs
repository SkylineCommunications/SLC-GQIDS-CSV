using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using CsvHelper;
using CsvHelper.Configuration;
using Skyline.DataMiner.Analytics.GenericInterface;

[GQIMetaData(Name = "From CSV")]
public class CSVDataSource : IGQIDataSource, IGQIInputArguments
{
	private readonly GQIStringArgument _pathArg;
	private readonly GQIStringArgument _delimiterArg;
	private GQIColumn[] _columns;
	private GQIRow[] _rows;

	public CSVDataSource()
	{
		_pathArg = new GQIStringArgument("File path") {
			IsRequired = true
		};
		_delimiterArg = new GQIStringArgument("Delimiter")
		{
			DefaultValue = ","
		};
	}

	public GQIArgument[] GetInputArguments()
	{
		return new GQIArgument[] {
			_pathArg,
			_delimiterArg
		};
	}

	public OnArgumentsProcessedOutputArgs OnArgumentsProcessed(OnArgumentsProcessedInputArgs args)
	{
		_columns = null;
		_rows = null;
		var csvFilePath = args.GetArgumentValue(_pathArg);

		if (string.IsNullOrEmpty(csvFilePath))
			return default;

		if (!csvFilePath.EndsWith(".csv"))
			return default;

		if (!File.Exists(csvFilePath))
			return default;

		var delimiter = args.GetArgumentValue(_delimiterArg) ?? ",";

		(_columns, _rows) = ReadCSVFile(csvFilePath, delimiter);

		return new OnArgumentsProcessedOutputArgs();
	}

	public GQIColumn[] GetColumns()
	{
		return _columns;
	}

	public GQIPage GetNextPage(GetNextPageInputArgs args)
	{
		return new GQIPage(_rows);
	}

	private (GQIColumn[] columns, GQIRow[] rows) ReadCSVFile(string path, string delimiter)
	{
		var config = new CsvConfiguration(CultureInfo.InvariantCulture)
		{
			Delimiter = delimiter
		};

		using (var streamReader = new StreamReader(path))
		using (var csvReader = new CsvReader(streamReader, config))
		{
			csvReader.Read();
			csvReader.ReadHeader();
			var columns = GetColumns(csvReader.HeaderRecord);
			var columnTypes = GetColumnTypes(columns);
			var rows = GetRows(csvReader, columnTypes);
			return (columns, rows);
		}
	}

	private GQIColumn[] GetColumns(string[] header)
	{
		return header.Select(GetColumn).ToArray();
	}

	private GQIColumn GetColumn(string head)
	{
		var columnInfo = GetColumnInfo(head);
		switch (columnInfo.type)
		{
			case "bool": return new GQIBooleanColumn(columnInfo.name);
			case "datetime": return new GQIDateTimeColumn(columnInfo.name);
			case "double": return new GQIDoubleColumn(columnInfo.name);
			case "int": return new GQIIntColumn(columnInfo.name);
			default: return new GQIStringColumn(columnInfo.name);
		}
	}

	private Type[] GetColumnTypes(GQIColumn[] columns)
	{
		return columns.Select(column => GetColumnType(column.Type)).ToArray();
	}

	private Type GetColumnType(GQIColumnType type)
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

	private (string name, string type) GetColumnInfo(string head)
	{
		var separatorIndex = head.IndexOf("::");
		if (separatorIndex == -1)
			return (head, "string");

		var name = head.Substring(0, separatorIndex);
		var type = head.Substring(separatorIndex + 2);
		return (name, type);
	}

	private GQIRow[] GetRows(CsvReader reader, Type[] columnTypes)
	{
		var rows = new List<GQIRow>();
		while (reader.Read())
		{
			var row = GetRow(reader, columnTypes);
			rows.Add(row);
		}
		return rows.ToArray();
	}

	private GQIRow GetRow(CsvReader reader, Type[] columnTypes)
	{
		var cells = columnTypes.Select((type, index) => GetCell(reader, index, type));
		return new GQIRow(cells.ToArray());
	}

	private GQICell GetCell(CsvReader reader, int index, Type type)
	{
		var value = reader.GetField(type, index);
		return new GQICell() { Value = value };
	}
}