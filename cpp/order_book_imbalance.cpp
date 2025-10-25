#include <pybind11/pybind11.h>
#include <pybind11/numpy.h>

namespace py = pybind11;

py::array_t<double> order_book_imbalance(py::array_t<double, py::array::c_style | py::array::forcecast> bid_input,
                                         py::array_t<double, py::array::c_style | py::array::forcecast> ask_input) {
    auto bid = bid_input.unchecked<1>();
    auto ask = ask_input.unchecked<1>();

    if (bid.shape(0) != ask.shape(0)) {
        throw std::runtime_error("bid_size and ask_size must have the same length");
    }

    py::array_t<double> output(bid.shape(0));
    auto out = output.mutable_unchecked<1>();

    for (py::ssize_t index = 0; index < bid.shape(0); ++index) {
        const double total_depth = bid(index) + ask(index);
        if (total_depth > 0.0) {
            out(index) = (bid(index) - ask(index)) / total_depth;
        } else {
            out(index) = 0.0;
        }
    }

    return output;
}

PYBIND11_MODULE(market_pipeline_native, module) {
    module.doc() = "Native helpers for the market pipeline";
    module.def("order_book_imbalance", &order_book_imbalance, "Compute order-book imbalance for bid/ask sizes");
}
