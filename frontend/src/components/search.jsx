import * as React from 'react';
import TextField from '@mui/material/TextField';
import Autocomplete from '@mui/material/Autocomplete';
import CircularProgress from '@mui/material/CircularProgress';
import axios from 'axios';
import Button from '@mui/material/Button';
import Container from '@mui/material/Container';
import SearchIcon from '@mui/icons-material/Search';
import Table from '@mui/material/Table';
import TableBody from '@mui/material/TableBody';
import TableCell from '@mui/material/TableCell';
import TableContainer from '@mui/material/TableContainer';
import TableHead from '@mui/material/TableHead';
import TableRow from '@mui/material/TableRow';
import Paper from '@mui/material/Paper';
import toast, { Toaster } from 'react-hot-toast';


function sleep(delay = 0) {
    return new Promise((resolve) => {
        setTimeout(resolve, delay);
    });
}

export default function Asynchronous() {
    const [open, setOpen] = React.useState(false);
    const [options, setOptions] = React.useState([]);
    const loading = open && options.length === 0;
    const [loadingData, setLoadingData] = React.useState(false);
    const [userData, setUserData] = React.useState(null);
    const [selectedOption, setSelectedOption] = React.useState(null); // New state to store the selected option

    const postDataToApi = async () => {
        try {
            const dataToSend = {
                Firstname: userData.Firstname ?? "",
                Lastname: userData.Lastname ?? "",
                Gender: userData.gender ?? "",
                DOB: userData.DOB ?? "",
                PhoneNumber: userData.PhoneNumber ?? "",
                Streetaddress: userData.streetaddress ?? "",
                Zipcode: userData.zipcode ?? "",
                City: userData.city ?? "",
                State: userData.state ?? "",
                Case_Description: userData.description_data ?? "",
                Referred_By_Name: userData.referredby_name ?? ""
            };
            console.log(dataToSend)
            const response = await axios.post("http://0.0.0.0:5000/proxy-to-strataemr", dataToSend)
            if (response.status === 200) {
                // console.log(response.text())
                toast.success("Patient added successfully!");
            }


        } catch (error) {
            console.error("Patient data not added!", error);
            toast.error("Error adding patient data!");
        }
    };

    React.useEffect(() => {
        let active = true;

        if (!loading) {
            return undefined;
        }

        (async () => {
            await sleep(1e3); 

            if (active) {
                fetchData();
            }
        })();

        return () => {
            active = false;
        };
    }, [loading]);

    React.useEffect(() => {
        if (!open) {
            setOptions([]);
        }
    }, [open]);

    const fetchData = async () => {
        try {
            const response = await axios.get("http://10.0.0.189:5000/load_data");
            if (response?.data?.data) {
                const fullNames = response.data.data.map((item) => ({
                    title: item.Lastname + " " + item.Firstname,
                    id: item.PatientID,
                }));
                setOptions(fullNames);
            }
        } catch (error) {
            console.error("Error fetching data:", error);
        }
    };
    const fetchUserData = async () => {
        try {
            setLoadingData(true);
            if (selectedOption) {
                const userDataResponse = await axios.get(`http://10.0.0.189:5000/user_data/${selectedOption.id}`);
                setUserData(userDataResponse.data);
            }
            setLoadingData(false);
        } catch (error) {
            console.error("Error fetching user data:", error);
            setLoadingData(false);
        }
    };
    return (
        <>
            <Container sx={{ display: 'flex', alignItems: 'center', marginTop: '2rem' }}>
                <Toaster position="top-right"
                    reverseOrder={false} />

                <Autocomplete
                    id="asynchronous-demo"
                    sx={{ flexGrow: 1 }}
                    open={open}
                    onOpen={() => {
                        setOpen(true);
                    }}
                    onClose={() => {
                        setOpen(false);
                    }}
                    freeSolo
                    isOptionEqualToValue={(option, value) => option.title === value.title}
                    getOptionLabel={(option) => option.title}
                    options={options}
                    loading={loading}

                    onChange={(event, newValue) => {
                        setSelectedOption(newValue); // Update the selected option state
                        setOpen(false);
                        setOptions(newValue ? [newValue, ...options] : options);
                    }}

                    renderInput={(params) => (
                        <TextField
                            {...params}
                            label="Search User"
                            InputProps={{
                                ...params.InputProps,
                                endAdornment: (
                                    <React.Fragment>
                                        {loading ? <CircularProgress color="inherit" size={20} /> : null}
                                        {params.InputProps.endAdornment}
                                    </React.Fragment>
                                ),
                                startAdornment: (
                                    <React.Fragment>
                                        <SearchIcon color="inherit" sx={{ mr: 1 }} />
                                        {params.InputProps.startAdornment}
                                    </React.Fragment>
                                ),
                            }}
                        />
                    )}
                />
                <Button variant="contained" color="primary" onClick={() => { fetchUserData() }} sx={{ ml: 2 }}  >
                    Search
                </Button>
                {loadingData && (
                    <CircularProgress color="primary" size={24} sx={{ ml: 2 }} />
                )}


            </Container>
            <Container sx={{ display: 'flex', alignItems: 'center', marginTop: '2rem' }}>
                {userData && (
                    <>
                        <TableContainer component={Paper} sx={{ marginTop: '2rem' }}>
                            <Table>
                                <TableHead>
                                    <TableRow>
                                        <TableCell>Firstname</TableCell>
                                        <TableCell>Lastname</TableCell>
                                        <TableCell>Gender</TableCell>
                                        <TableCell>DOB</TableCell>
                                        <TableCell>PatientID</TableCell>
                                        <TableCell>PhoneNumber</TableCell>
                                        <TableCell>Streetaddress</TableCell>
                                        <TableCell>Zipcode</TableCell>
                                        <TableCell>City</TableCell>
                                        <TableCell>State</TableCell>
                                        <TableCell>Case_Description</TableCell>
                                        <TableCell>Case_Description_date</TableCell>
                                        <TableCell>referred_by_name</TableCell>
                                        <TableCell>policy_payer</TableCell>
                                        <TableCell>subscriber_id</TableCell>

                                    </TableRow>
                                </TableHead>
                                <TableBody>
                                    <TableRow>
                                        <TableCell>{userData.Firstname}</TableCell>
                                        <TableCell>{userData.Lastname}</TableCell>
                                        <TableCell>{userData.gender}</TableCell>
                                        <TableCell>{userData.DOB}</TableCell>
                                        <TableCell>{userData.PatientID}</TableCell>
                                        <TableCell>{userData.PhoneNumber}</TableCell>
                                        <TableCell>{userData.streetaddress}</TableCell>
                                        <TableCell>{userData.zipcode}</TableCell>
                                        <TableCell>{userData.city}</TableCell>
                                        <TableCell>{userData.state}</TableCell>
                                        <TableCell>{userData.description_data}</TableCell>
                                        <TableCell>{userData.description_date}</TableCell>
                                        <TableCell>{userData.referredby_name}</TableCell>
                                        <TableCell>{userData.policy_payer}</TableCell>
                                        <TableCell>{userData.policy_subscriber_id}</TableCell>

                                    </TableRow>
                                </TableBody>
                            </Table>
                        </TableContainer>

                        <Button variant="contained" color="primary" sx={{ ml: 2 }} onClick={postDataToApi}>
                            Post
                        </Button>
                    </>
                )}
            </Container>
        </>
    );
}






