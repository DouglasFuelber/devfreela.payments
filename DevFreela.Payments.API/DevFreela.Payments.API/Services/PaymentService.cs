using DevFreela.Payments.API.Models;
using System.Threading.Tasks;

namespace DevFreela.Payments.API.Services
{
    public class PaymentService : IPaymentService
    {
        public Task<bool> Process(PaymentInfoInputModel payment)
        {
            return Task.FromResult(true);
        }
    }
}
