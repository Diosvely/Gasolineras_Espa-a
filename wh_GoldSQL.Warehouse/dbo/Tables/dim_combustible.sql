CREATE TABLE [dbo].[dim_combustible] (

	[IDCombustible] bigint NULL, 
	[TipoCombustible] varchar(8000) NULL, 
	[Categoria] varchar(8000) NULL, 
	[EsPremium] bit NULL, 
	[EsRenovable] bit NULL
);