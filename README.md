# P-BRID and SP-BRID result diversification algorithms in a Map-Reduce framework

**This repository contains the source code to the implementation reported in the manuscript _Adding result diversification to kNN-based joins in a Map-Reduce framework_, submitted to the 34th DEXA conference.**


## Experiment setup 

The implementations of both _P-BRID_ and _SP-BRID_ algorithms can be found inside the _src_ folder. The code is implemented in Java version 1.8.0_202 with the Java Development Kit (JDK) version 8.

The experiments were executed in our local cluster, a QLustar server with two nodes, each with 48 AMD Opteron 2.2GHz hyper-thread cores, 94GB of RAM, a 1 TB SATA hard drive and a dedicated JVM process reserved for the experiments in a pseudo distributed environment in Apache Hadoop.   


## Datasets

The datasets used in experiments is described as follows.

| Dataset           | Description                                        |    Available at                                                                   |
|-------------------| -------------------------------------------------- | ----------------------------------------------------------------------------------|
| CITIES            | Coordinates of US cities.	                         |      https://public.opendatasoft.com/explore/dataset/us-cities-demographics/      |
| NASA              | Low-level features from satellite images.          |      http://www.sisap.org/dbs.html                                                |
| GAUSS             | Synthetic iid dimensions (Standard distribution).  |        ---                                                                        |
| UNIFORM           | Synthetic iid dimensions (Uniform distribution).   |        ---                                                                        |
| MNIST             | Handwritten digits.	                             |      http://yann.lecun.com/exdb/mnist/                                            |
| ALOI              | 3D color model images.                             |      https://aloi.science.uva.nl/                                                 |                
| COLORS            | Low-level features from color photos.              |      http://www.sisap.org/dbs.html                                                |  
| SIFT              | SIFT features from images.                         |      http://corpus-texmex.irisa.fr/                                               |
                                           


## Notes

_(C) THIS SOFTWARE IS PROVIDED ``AS IS'' AND ANY EXPRESSED OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL THE AUTHORS OF THIS SOFTWARE OR ITS CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE._
