using Microsoft.AspNetCore.Http;
using System.Threading.Tasks;

namespace Aspnet_Core_Essential
{
	public class MyCustomMiddleware
	{
		// 1. Middleware must have RequestDelegate _link2next:
		private readonly RequestDelegate _next;

		// 2. public Ctor :
		public MyCustomMiddleware(RequestDelegate nextMiddleWare)
		{
			_next = nextMiddleWare;
		}

		// 3. Method named as Invoke/InvokeAsync :
		public async Task InvokeAsync(HttpContext httpContext)
		{
			if (httpContext.Request.Method == "POST")
			{
				await httpContext.Response.WriteAsync("it was POST request");
			}
			else
			{
				await _next.Invoke(httpContext);
			}
		}

		// 4. Using - app.UseMiddleware < MyCustomMiddleware > ();
	}
}
