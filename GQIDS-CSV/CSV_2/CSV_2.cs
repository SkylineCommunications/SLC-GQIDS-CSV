using System;
using System.CodeDom;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using CsvHelper;
using CsvHelper.Configuration;
using CsvHelper.TypeConversion;
using Skyline.DataMiner.Analytics.GenericInterface;

[GQIMetaData(Name = "From CSV")]
public class CSVDataSource : IGQIDataSource, IGQIInputArguments, IGQIUpdateable
{
	private const string CSV_ROOT_PATH = @"C:\Skyline DataMiner\Documents";

	private readonly DateTimeConverter _dateTimeConverter;
	private readonly GQIStringDropdownArgument _fileArgument;
	private readonly GQIStringArgument _delimiterArgument;

	private HeaderInfo _headerInfo;
	private GQIRow[] _rows;
	private int _rowCount;
	private string _csvFilePath;
	private string _delimiter;

	private IGQIUpdater _updater;
	private FileSystemWatcher _watcher;


	public CSVDataSource()
	{
		_dateTimeConverter = new DateTimeConverter();

		var csvFileOptions = GetCsvFileOptions();
		_fileArgument = new GQIStringDropdownArgument("File", csvFileOptions)
		{
			IsRequired = true
		};

		_delimiterArgument = new GQIStringArgument("Delimiter")
		{
			DefaultValue = ","
		};
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

	public GQIArgument[] GetInputArguments()
	{
		return new GQIArgument[] {
			_fileArgument,
			_delimiterArgument,
		};
	}

	public OnArgumentsProcessedOutputArgs OnArgumentsProcessed(OnArgumentsProcessedInputArgs args)
	{
		_headerInfo = default;
		_rows = null;
		var csvFileOption = args.GetArgumentValue(_fileArgument);

		if (string.IsNullOrEmpty(csvFileOption))
			throw new GenIfException("Missing csv file.");

		var relativeFileName = csvFileOption.Replace("/", @"\");
		_csvFilePath = $@"{CSV_ROOT_PATH}\{relativeFileName}.csv";

		if (!File.Exists(_csvFilePath))
			throw new GenIfException($"Csv file does not exist: {_csvFilePath}");

		_delimiter = args.GetArgumentValue(_delimiterArgument);
		if (string.IsNullOrEmpty(_delimiter))
			_delimiter = ",";

		ReadCSVFile();

		return default;
	}

	public GQIColumn[] GetColumns()
	{
		return _headerInfo.Columns;
	}

	public GQIPage GetNextPage(GetNextPageInputArgs args)
	{
		return new GQIPage(_rows);
	}


	private void ReadCSVFile()
	{
		var config = new CsvConfiguration(CultureInfo.InvariantCulture)
		{
			Delimiter = _delimiter
		};

		using (var fileStream = new FileStream(_csvFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete))
		using (var streamReader = new StreamReader(fileStream))
		using (var csvReader = new CsvReader(streamReader, config))
		{
			csvReader.Read();
			csvReader.ReadHeader();
			_headerInfo = GetHeaderInfo(csvReader.HeaderRecord);
			_rows = ReadRows(csvReader);
			_rowCount = _rows.Length;
		}
	}

	private void UpdateRows()
	{
		var config = new CsvConfiguration(CultureInfo.InvariantCulture)
		{
			Delimiter = _delimiter
		};

		using (var fileStream = new FileStream(_csvFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete))
		using (var streamReader = new StreamReader(fileStream))
		using (var csvReader = new CsvReader(streamReader, config))
		{
			csvReader.Read(); // Skip header
			var columnTypes = _headerInfo.GetColumnTypes();
			UpdateRows(csvReader, columnTypes);
		}
	}


	private GQIRow[] ReadRows(CsvReader csvReader)
	{
		var columnTypes = _headerInfo.GetColumnTypes();
		return ReadRows(csvReader, columnTypes);
	}


	private HeaderInfo GetHeaderInfo(string[] header)
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

	private GQIColumn GetColumn(string name, string type)
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

	private (string name, string type) GetColumnInfo(string head)
	{
		var separatorIndex = head.IndexOf("::");
		if (separatorIndex == -1)
			return (head, "string");

		var name = head.Substring(0, separatorIndex);
		var type = head.Substring(separatorIndex + 2);
		return (name, type);
	}

	private GQIRow[] ReadRows(CsvReader reader, Type[] columnTypes)
	{
		var rows = new List<GQIRow>();
		while (reader.Read())
		{
			var row = ReadRow(reader, columnTypes, rows.Count);
			rows.Add(row);
		}
		return rows.ToArray();
	}

	private void UpdateRows(CsvReader reader, Type[] columnTypes)
	{
		// Add or update new rows
		var index = 0;
		while (reader.Read())
		{
			var row = ReadRow(reader, columnTypes, index++);
			if (index <= _rowCount)
				_updater.UpdateRow(row);
			else
				_updater.AddRow(row);
		}

		// Delete old rows
		while (_rowCount > index)
		{
			_updater.RemoveRow($"{--_rowCount}");
		}
	}

	private GQIRow ReadRow(CsvReader reader, Type[] columnTypes, int key)
	{
		var cells = columnTypes.Select((type, index) => GetCell(reader, index, type));
		return new GQIRow(key.ToString(), cells.ToArray());
	}

	private GQIRow ReadRow(CsvReader reader, int keyIndex, Type[] columnTypes)
	{
		var key = reader.GetField(keyIndex);
		var cells = columnTypes.Select((type, index) => GetCell(reader, index, type));
		return new GQIRow(key, cells.ToArray());
	}

	private GQICell GetCell(CsvReader reader, int index, Type type)
	{
		if (type == typeof(DateTime))
		{
			var dateTime = reader.GetField<DateTime>(index, _dateTimeConverter);
			return new GQICell() { Value = dateTime };
		}

		var value = reader.GetField(type, index);
		return new GQICell() { Value = value };
	}

	public void OnStartUpdates(IGQIUpdater updater)
	{
		_updater = updater;
		var directory = Path.GetDirectoryName(_csvFilePath);
		var fileName = Path.GetFileName(_csvFilePath);
		_watcher = new FileSystemWatcher(directory, fileName);
		_watcher.NotifyFilter = NotifyFilters.LastWrite;
		_watcher.Changed += OnChanged;
		_watcher.EnableRaisingEvents = true;
	}

	public void OnStopUpdates()
	{
		if (_watcher is null)
			return;

		_watcher.Changed -= OnChanged;
		_watcher.Dispose();
		_watcher = null;
	}

	private void OnChanged(object sender, FileSystemEventArgs args)
	{
		try
		{
			UpdateRows();
		}
		catch (Exception ex)
		{
			throw new GenIfException($"Failed to update rows: {ex.Message}");
		}
	}

	private class HeaderInfo
	{
		public int KeyIndex { get; }
		public GQIColumn[] Columns { get; }

		public HeaderInfo(int keyIndex, GQIColumn[] columns)
		{
			KeyIndex = keyIndex;
			Columns = columns;
		}

		public Type[] GetColumnTypes()
		{
			return Columns.Select(column => GetColumnType(column.Type)).ToArray();
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
	}

	private class DateTimeConverter : ITypeConverter
	{
		public object ConvertFromString(string text, IReaderRow row, MemberMapData memberMapData)
		{
			try
			{
				return DateTime.SpecifyKind(DateTime.Parse(text), DateTimeKind.Utc);
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
}