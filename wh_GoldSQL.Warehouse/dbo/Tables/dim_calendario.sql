CREATE TABLE [dbo].[dim_calendario] (

	[Fecha] date NULL, 
	[Anio] int NULL, 
	[Mes] int NULL, 
	[Dia] int NULL, 
	[Trimestre] int NULL, 
	[DiaSemana] int NULL, 
	[NombreMes] varchar(8000) NULL, 
	[AnioMes] varchar(8000) NULL
);