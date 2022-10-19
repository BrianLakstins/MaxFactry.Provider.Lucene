rem Package the library for Nuget
copy ..\MaxFactry.Provider.LuceneProvider-NF-4.5.2\bin\Release\MaxFactry.Provider.Lucene*.dll lib\net452\

c:\install\nuget\nuget.exe pack MaxFactry.Provider.Lucene.nuspec -OutputDirectory "packages" -IncludeReferencedProjects -properties Configuration=Release 